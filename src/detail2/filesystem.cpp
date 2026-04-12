#include "filesystem.hpp"

#include "../detail/win32_util.hpp"

#include <algorithm>
#include <chrono>
#include <deque>
#include <map>
#include <set>
#include <stdexcept>

namespace soratransport::detail2 {

namespace {

constexpr std::int64_t kWindowsToUnixEpochOffset100ns = 116444736000000000ll;

struct DirectoryWorkItem {
	std::filesystem::path root_path;
	std::filesystem::path archive_root_name;
	std::filesystem::path directory_path;
};

FileTimestamp filetime_to_timestamp(LARGE_INTEGER value) {
	const auto unix_ticks = value.QuadPart - kWindowsToUnixEpochOffset100ns;
	auto seconds = unix_ticks / 10000000ll;
	auto remaining_ticks = unix_ticks % 10000000ll;
	if (remaining_ticks < 0) {
		remaining_ticks += 10000000ll;
		--seconds;
	}

	return FileTimestamp{seconds, static_cast<long>(remaining_ticks * 100ll)};
}

void maybe_set_timestamp(std::optional<FileTimestamp>& destination, LARGE_INTEGER value) {
	if (value.QuadPart <= 0) {
		return;
	}
	destination = filetime_to_timestamp(value);
}

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

void populate_file_status(FileMeta& meta) {
	meta.status = std::filesystem::symlink_status(meta.full_path);
	meta.size = 0;
	if (std::filesystem::is_regular_file(meta.status)) {
		std::error_code size_error;
		meta.size = std::filesystem::file_size(meta.full_path, size_error);
		if (size_error) {
			meta.size = 0;
		}
	}
}

void populate_file_meta_from_handle(FileMeta& meta, HANDLE handle) {
	FILE_BASIC_INFO basic_info{};
	if (::GetFileInformationByHandleEx(handle, FileBasicInfo, &basic_info, static_cast<DWORD>(sizeof(basic_info)))) {
		meta.windows_file_attributes = basic_info.FileAttributes;
		maybe_set_timestamp(meta.creation_time, basic_info.CreationTime);
		maybe_set_timestamp(meta.last_access_time, basic_info.LastAccessTime);
		maybe_set_timestamp(meta.last_write_time, basic_info.LastWriteTime);
		maybe_set_timestamp(meta.change_time, basic_info.ChangeTime);
	}

	FILE_STANDARD_INFO standard_info{};
	if (::GetFileInformationByHandleEx(handle, FileStandardInfo, &standard_info, static_cast<DWORD>(sizeof(standard_info)))
		&& std::filesystem::is_regular_file(meta.status)) {
		meta.size = static_cast<std::uint64_t>(std::max<LONGLONG>(0, standard_info.EndOfFile.QuadPart));
	}
}

void populate_file_meta(FileMeta& meta) {
	populate_file_status(meta);

	UniqueWin32Handle handle(::CreateFileW(
		meta.full_path.c_str(),
		FILE_READ_ATTRIBUTES,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
		nullptr));
	if (handle.valid()) {
		populate_file_meta_from_handle(meta, handle.get());
	} else {
		const auto attributes = ::GetFileAttributesW(meta.full_path.c_str());
		if (attributes != INVALID_FILE_ATTRIBUTES) {
			meta.windows_file_attributes = attributes;
		}
	}
	meta.symlink_target.reset();
	if (meta.status.type() == std::filesystem::file_type::symlink) {
		return;
	}
}

std::filesystem::path archive_root_name_for_source(const std::filesystem::path& source_path) {
	auto normalized = source_path.lexically_normal();
	auto name = normalized.filename();
	if (!name.empty()) {
		return name;
	}

	auto root_name = normalized.root_name().wstring();
	if (!root_name.empty()) {
		std::replace(root_name.begin(), root_name.end(), L':', L'_');
		return std::filesystem::path(root_name);
	}

	throw std::runtime_error("source path must have a name in the archive");
}

std::string archive_path_for_entry(const std::filesystem::path& archive_root_name, const std::filesystem::path& relative_path) {
	if (relative_path.empty() || relative_path == ".") {
		return path_to_generic_utf8_string(archive_root_name);
	}
	return path_to_generic_utf8_string(archive_root_name / relative_path);
}

bool is_future_ready(std::future<OpenedFile>& future) {
	return future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
}

std::size_t reader_buffer_size(std::uint64_t file_size, const SlotTuning& slot_tuning) {
	const auto bounded_size = file_size == 0
		? static_cast<std::uint64_t>(1)
		: std::min<std::uint64_t>(file_size, slot_tuning.max_slot_bytes);
	return std::max<std::size_t>(1, static_cast<std::size_t>(bounded_size));
}

OpenedFile open_regular_file(
	BufferPool& pool,
	FileMeta meta,
	const PipelineTuning& tuning,
	CancelEvent* cancel_event,
	SemaphoreCor::Guard open_guard) {
	throw_if_cancelled(cancel_event);

	UniqueWin32Handle handle(::CreateFileW(
		meta.full_path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_OVERLAPPED,
		nullptr));
	if (!handle.valid()) {
		throw make_win32_error("failed to open input file: " + path_to_utf8_string(meta.full_path));
	}

	populate_file_meta_from_handle(meta, handle.get());
	throw_if_cancelled(cancel_event);

	OpenedFile opened_file;
	opened_file.meta = std::move(meta);
	auto reader = SequentialFileReader(
		pool,
		opened_file.meta.full_path,
		opened_file.meta.size,
		reader_buffer_size(opened_file.meta.size, tuning.reader_slots),
		handle.release());
	if (cancel_event != nullptr) {
		reader.listenCancelSignal(*cancel_event);
	}
	opened_file.reader.emplace(std::move(reader));
	opened_file.open_guard = std::move(open_guard);
	return opened_file;
}

} // namespace

SequentialFileReader::SequentialFileReader(
	BufferPool& pool,
	const std::filesystem::path& path,
	std::uint64_t size,
	std::size_t buffer_size,
	HANDLE handle)
	: reader_(std::make_unique<OverlappedFileReader>(pool, path, size, buffer_size, handle)) {}

SequentialFileReader::~SequentialFileReader() = default;

SequentialFileReader::SequentialFileReader(SequentialFileReader&& other) = default;

SequentialFileReader& SequentialFileReader::operator=(SequentialFileReader&& other) {
	if (this != &other) {
		reader_ = std::move(other.reader_);
	}
	return *this;
}

void SequentialFileReader::listenCancelSignal(CancelEvent& event) {
	if (reader_ != nullptr) {
		reader_->listenCancelSignal(event);
	}
}

void SequentialFileReader::start_prefetch(std::size_t max_bytes) {
	if (reader_ == nullptr) {
		throw std::runtime_error("file reader is closed");
	}
	reader_->start_prefetch(max_bytes);
}

DataChunk SequentialFileReader::read_next_chunk() {
	if (reader_ == nullptr) {
		throw std::runtime_error("file reader is closed");
	}
	return reader_->read_next_chunk();
}

std::uint64_t SequentialFileReader::offset() const {
	return reader_ == nullptr ? 0 : reader_->offset();
}

bool SequentialFileReader::eof() const {
	return reader_ == nullptr || reader_->eof();
}

bool SequentialFileReader::is_open() const {
	return reader_ != nullptr && reader_->is_open();
}

bool SequentialFileReader::is_cancelled() const {
	return reader_ != nullptr && reader_->is_cancelled();
}

void SequentialFileReader::cancel_pending_work() {
	if (reader_ != nullptr) {
		reader_->cancel_pending_work();
	}
}

FileTraverser::FileTraverser(boost::asio::any_io_executor executor, PipelineTuning tuning, CancelEvent* cancel_event)
	: tuning_(std::move(tuning)),
	  cancel_event_(cancel_event),
	  directory_slots_(std::move(executor), std::max<std::size_t>(1, tuning_.directory_traversal_concurrency)) {
	if (cancel_event_ != nullptr) {
		cancel_connection_ = cancel_event_->connect([this] {
			directory_slots_.cancel("directory traversal cancelled");
		});
	}
}

boost::asio::awaitable<void> FileTraverser::traverse(const std::filesystem::path& root, BoundedQueue<TraversalEntry>& out_queue) const {
	std::vector<std::filesystem::path> roots;
	roots.push_back(root);
	co_await traverse(roots, out_queue);
	co_return;
}

boost::asio::awaitable<void> FileTraverser::traverse(const std::vector<std::filesystem::path>& roots, BoundedQueue<TraversalEntry>& out_queue) const {
	throw_if_cancelled(cancel_event_);
	if (roots.empty()) {
		throw std::runtime_error("at least one source path is required");
	}

	try {
		std::size_t next_sequence = 0;
		std::set<std::string> archive_root_names;

		for (const auto& root_path : roots) {
			throw_if_cancelled(cancel_event_);
			if (!std::filesystem::exists(root_path)) {
				throw std::runtime_error("source path does not exist: " + path_to_utf8_string(root_path));
			}

			const auto archive_root_name = archive_root_name_for_source(root_path);
			const auto archive_root_key = path_to_generic_utf8_string(archive_root_name);
			if (!archive_root_names.insert(archive_root_key).second) {
				throw std::runtime_error("duplicate archive root name is not supported: " + archive_root_key);
			}

			if (std::filesystem::is_directory(root_path)) {
				TraversalEntry root_entry;
				root_entry.meta.full_path = root_path;
				populate_file_meta(root_entry.meta);
				root_entry.meta.relative_path_in_tar = archive_path_for_entry(archive_root_name, ".");
				root_entry.sequence = next_sequence++;
				co_await out_queue.async_push_await(std::move(root_entry));

				std::deque<DirectoryWorkItem> directories;
				directories.push_back(DirectoryWorkItem{root_path, archive_root_name, root_path});
				while (!directories.empty()) {
					throw_if_cancelled(cancel_event_);
					auto directory_guard = co_await directory_slots_.acquire(1);
					auto current = std::move(directories.front());
					directories.pop_front();

					for (const auto& entry : std::filesystem::directory_iterator(current.directory_path)) {
						throw_if_cancelled(cancel_event_);
						FileMeta meta;
						meta.full_path = entry.path();
						populate_file_status(meta);
						if (meta.status.type() == std::filesystem::file_type::symlink) {
							continue;
						}
						meta.relative_path_in_tar = archive_path_for_entry(
							current.archive_root_name,
							entry.path().lexically_relative(current.root_path));
						if (meta.relative_path_in_tar.empty()) {
							continue;
						}

						TraversalEntry output_entry;
						output_entry.meta = std::move(meta);
						output_entry.sequence = next_sequence++;
						output_entry.regular_file = output_entry.meta.status.type() == std::filesystem::file_type::regular;
						if (!output_entry.regular_file) {
							populate_file_meta(output_entry.meta);
						}
						if (entry.is_directory()) {
							directories.push_back(DirectoryWorkItem{current.root_path, current.archive_root_name, entry.path()});
						}

						co_await out_queue.async_push_await(std::move(output_entry));
					}
				}
				continue;
			}

			if (std::filesystem::is_regular_file(root_path)) {
				TraversalEntry entry;
				entry.meta.full_path = root_path;
				populate_file_status(entry.meta);
				entry.meta.relative_path_in_tar = path_to_generic_utf8_string(archive_root_name);
				entry.sequence = next_sequence++;
				entry.regular_file = true;
				co_await out_queue.async_push_await(std::move(entry));
				continue;
			}

			throw std::runtime_error("unsupported source path type: " + path_to_utf8_string(root_path));
		}
		out_queue.close();
	} catch (...) {
		out_queue.close();
		throw;
	}

	co_return;
}

FileOpener::FileOpener(BufferPool& pool, TaskExecutor& executor, PipelineTuning tuning, CancelEvent* cancel_event)
	: pool_(pool),
	  executor_(executor),
	  tuning_(std::move(tuning)),
	  cancel_event_(cancel_event),
	  open_slots_(executor.executor(), std::max<std::size_t>(1, tuning_.file_open_concurrency)) {
	if (cancel_event_ != nullptr) {
		cancel_connection_ = cancel_event_->connect([this] {
			open_slots_.cancel("file open cancelled");
		});
	}
}

boost::asio::awaitable<void> FileOpener::open(BoundedQueue<TraversalEntry>& in_entries, BoundedQueue<OpenedFile>& out_opened) const {
	try {
		std::map<std::size_t, std::future<OpenedFile>> pending_results;
		std::map<std::size_t, OpenedFile> ready_results;
		std::size_t next_emit_sequence = 0;

		auto emit_ready = [&](bool wait_for_next) -> boost::asio::awaitable<void> {
			while (true) {
				auto ready_it = ready_results.find(next_emit_sequence);
				if (ready_it != ready_results.end()) {
					auto opened_file = std::move(ready_it->second);
					ready_results.erase(ready_it);
					++next_emit_sequence;
					co_await out_opened.async_push_await(std::move(opened_file));
					continue;
				}

				auto pending_it = pending_results.find(next_emit_sequence);
				if (pending_it == pending_results.end()) {
					break;
				}
				if (!wait_for_next && !is_future_ready(pending_it->second)) {
					break;
				}

				ready_results.emplace(next_emit_sequence, pending_it->second.get());
				pending_results.erase(pending_it);
			}
		};

		while (auto entry = co_await in_entries.async_pop_await()) {
			throw_if_cancelled(cancel_event_);
			if (entry->regular_file) {
				auto open_guard = co_await open_slots_.acquire(1);
				const auto sequence = entry->sequence;
				pending_results.emplace(
					sequence,
					executor_.submit([
						this,
						meta = std::move(entry->meta),
						open_guard = std::move(open_guard)]() mutable {
							return open_regular_file(pool_, std::move(meta), tuning_, cancel_event_, std::move(open_guard));
						}));
			} else {
				OpenedFile opened_file;
				opened_file.meta = std::move(entry->meta);
				ready_results.emplace(entry->sequence, std::move(opened_file));
			}

			co_await emit_ready(false);
			while (pending_results.size() >= tuning_.file_open_concurrency) {
				co_await emit_ready(true);
			}
		}

		while (!pending_results.empty() || !ready_results.empty()) {
			co_await emit_ready(true);
		}
		out_opened.close();
	} catch (...) {
		out_opened.close();
		throw;
	}

	co_return;
}

FilePrefetcher::FilePrefetcher(boost::asio::any_io_executor executor, PipelineTuning tuning, CancelEvent* cancel_event)
	: tuning_(std::move(tuning)),
	  cancel_event_(cancel_event),
	  byte_budget_(std::move(executor), tuning_.file_prefetch_budget_bytes) {
	if (cancel_event_ != nullptr) {
		cancel_connection_ = cancel_event_->connect([this] {
			byte_budget_.cancel("file prefetch cancelled");
		});
	}
}

boost::asio::awaitable<void> FilePrefetcher::prefetch(BoundedQueue<OpenedFile>& in_opened, BoundedQueue<OpenedFile>& out_prefetched) const {
	try {
		while (auto opened_file = co_await in_opened.async_pop_await()) {
			throw_if_cancelled(cancel_event_);
			const auto budget_bytes = compute_prefetch_bytes(*opened_file, tuning_);
			if (budget_bytes > 0 && opened_file->reader.has_value()) {
				auto budget_guard = co_await byte_budget_.acquire(budget_bytes);
				opened_file->reader->start_prefetch(budget_bytes);
				opened_file->prefetched_bytes = budget_bytes;
				opened_file->prefetch_guard = std::move(budget_guard);
			}
			co_await out_prefetched.async_push_await(std::move(*opened_file));
		}
		out_prefetched.close();
	} catch (...) {
		out_prefetched.close();
		throw;
	}

	co_return;
}

std::size_t compute_prefetch_bytes(const OpenedFile& opened_file, const PipelineTuning& tuning) {
	if (!opened_file.reader.has_value() || !std::filesystem::is_regular_file(opened_file.meta.status) || opened_file.meta.size == 0) {
		return 0;
	}

	const auto max_bytes = tuning.reader_slots.max_slots * tuning.reader_slots.max_slot_bytes;
	return static_cast<std::size_t>(std::min<std::uint64_t>(opened_file.meta.size, max_bytes));
}

} // namespace soratransport::detail2