#include "pipeline.hpp"
#include "win32_util.hpp"

#include <algorithm>
#include <chrono>
#include <map>
#include <stdexcept>

#include <windows.h>

namespace soratransport {

namespace {

constexpr std::int64_t kWindowsToUnixEpochOffset100ns = 116444736000000000ll;

FileTimestamp filetime_to_timestamp(LARGE_INTEGER value) {
	const auto unix_ticks = value.QuadPart - kWindowsToUnixEpochOffset100ns;
	auto seconds = unix_ticks / 10000000ll;
	auto remaining_ticks = unix_ticks % 10000000ll;
	if (remaining_ticks < 0) {
		remaining_ticks += 10000000ll;
		--seconds;
	}

	return FileTimestamp{
		seconds,
		static_cast<long>(remaining_ticks * 100ll),
	};
}

void maybe_set_timestamp(std::optional<FileTimestamp>& destination, LARGE_INTEGER value) {
	if (value.QuadPart <= 0) {
		return;
	}
	destination = filetime_to_timestamp(value);
}

#if 0
std::string path_to_preserved_generic_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	return {utf8.begin(), utf8.end()};
}
#endif

struct UniqueWin32Handle {
	explicit UniqueWin32Handle(HANDLE input_handle = INVALID_HANDLE_VALUE) noexcept : handle(input_handle) {}
	~UniqueWin32Handle() {
		reset();
	}

	UniqueWin32Handle(const UniqueWin32Handle&) = delete;
	UniqueWin32Handle& operator=(const UniqueWin32Handle&) = delete;

	UniqueWin32Handle(UniqueWin32Handle&& other) noexcept : handle(other.release()) {}

	UniqueWin32Handle& operator=(UniqueWin32Handle&& other) noexcept {
		if (this != &other) {
			reset(other.release());
		}
		return *this;
	}

	void reset(HANDLE new_handle = INVALID_HANDLE_VALUE) noexcept {
		if (handle != INVALID_HANDLE_VALUE) {
			::CloseHandle(handle);
		}
		handle = new_handle;
	}

	HANDLE get() const noexcept {
		return handle;
	}

	HANDLE release() noexcept {
		const auto released = handle;
		handle = INVALID_HANDLE_VALUE;
		return released;
	}

	bool valid() const noexcept {
		return handle != INVALID_HANDLE_VALUE;
	}

	HANDLE handle = INVALID_HANDLE_VALUE;
};

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

#if 0
	if (meta.status.type() == std::filesystem::file_type::symlink) {
		std::error_code read_link_error;
		auto target = std::filesystem::read_symlink(meta.full_path, read_link_error);
		if (!read_link_error) {
			meta.symlink_target = path_to_preserved_generic_utf8_string(target);
		}
	}
#endif
}

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

std::size_t compute_reader_prefetch_bytes(
	const OpenedFileReader& opened_file,
	std::size_t buffer_size) {
	if (!opened_file.reader.has_value()
		|| !std::filesystem::is_regular_file(opened_file.meta.status)
		|| opened_file.meta.size == 0) {
		return 0;
	}

	return static_cast<std::size_t>(std::min<std::uint64_t>(
		opened_file.meta.size,
		static_cast<std::uint64_t>(std::max<std::size_t>(1, buffer_size)) * kOverlappedFileReadQueueDepth));
}

bool is_future_ready(std::future<OpenedFileReader>& future) {
	return future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
}

OpenedFileReader open_regular_file(
	BufferPool& pool,
	FileMeta meta,
	std::size_t buffer_size,
	CancelEvent* cancel_event) {
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

	OpenedFileReader opened_file;
	opened_file.meta = std::move(meta);
	auto reader = FileReader(
		pool,
		opened_file.meta.full_path,
		opened_file.meta.size,
		buffer_size,
		handle.release());
	if (cancel_event != nullptr) {
		reader.listenCancelSignal(*cancel_event);
	}
	opened_file.reader.emplace(std::move(reader));
	return opened_file;
}

std::filesystem::path archive_root_name_for_directory(const std::filesystem::path& root_dir) {
	auto normalized = root_dir.lexically_normal();
	auto name = normalized.filename();
	if (!name.empty()) {
		return name;
	}

	auto root_name = normalized.root_name().wstring();
	if (!root_name.empty()) {
		std::replace(root_name.begin(), root_name.end(), L':', L'_');
		return std::filesystem::path(root_name);
	}

	throw std::runtime_error("source directory must have a name in the archive");
}

std::string archive_path_for_entry(
	const std::filesystem::path& root_dir_name,
	const std::filesystem::path& path_relative_to_root) {
	if (path_relative_to_root.empty() || path_relative_to_root == ".") {
		return path_to_generic_utf8_string(root_dir_name);
	}
	return path_to_generic_utf8_string(root_dir_name / path_relative_to_root);
}

} // namespace

InFlightReadBudget::InFlightReadBudget(std::size_t max_bytes) : max_bytes_(std::max<std::size_t>(1, max_bytes)) {}

void InFlightReadBudget::listenCancelSignal(CancelEvent& event) {
	cancel_connection_ = event.connect([this] {
		cancelled_.store(true, std::memory_order_release);
		cv_.notify_all();
	});
}

void InFlightReadBudget::acquire(std::size_t bytes) {
	if (bytes == 0) {
		return;
	}
	if (bytes > max_bytes_) {
		throw std::runtime_error("requested read buffer exceeds configured in-flight read budget");
	}

	std::unique_lock lock(mutex_);
	cv_.wait(lock, [&] {
		return cancelled_.load(std::memory_order_acquire) || used_bytes_ + bytes <= max_bytes_;
	});
	if (cancelled_.load(std::memory_order_acquire)) {
		throw CancelledError("read budget acquisition cancelled");
	}
	used_bytes_ += bytes;
}

bool InFlightReadBudget::try_acquire(std::size_t bytes) {
	if (bytes == 0) {
		return true;
	}
	if (bytes > max_bytes_) {
		throw std::runtime_error("requested read buffer exceeds configured in-flight read budget");
	}

	std::lock_guard lock(mutex_);
	if (cancelled_.load(std::memory_order_acquire)) {
		throw CancelledError("read budget acquisition cancelled");
	}
	if (used_bytes_ + bytes > max_bytes_) {
		return false;
	}
	used_bytes_ += bytes;
	return true;
}

void InFlightReadBudget::release(std::size_t bytes) {
	if (bytes == 0) {
		return;
	}

	{
		std::lock_guard lock(mutex_);
		used_bytes_ -= std::min(used_bytes_, bytes);
	}
	cv_.notify_all();
}

std::size_t InFlightReadBudget::max_bytes() const {
	return max_bytes_;
}

std::size_t InFlightReadBudget::used_bytes() const {
	std::lock_guard lock(mutex_);
	return used_bytes_;
}

bool InFlightReadBudget::is_cancelled() const {
	return cancelled_.load(std::memory_order_acquire);
}

InFlightWriteBudget::InFlightWriteBudget(std::size_t max_bytes) : max_bytes_(std::max<std::size_t>(1, max_bytes)) {}

void InFlightWriteBudget::listenCancelSignal(CancelEvent& event) {
	cancel_connection_ = event.connect([this] {
		cancelled_.store(true, std::memory_order_release);
		cv_.notify_all();
	});
}

void InFlightWriteBudget::acquire(std::size_t bytes) {
	if (bytes == 0) {
		return;
	}
	if (bytes > max_bytes_) {
		throw std::runtime_error("requested write buffer exceeds configured in-flight write budget");
	}

	std::unique_lock lock(mutex_);
	cv_.wait(lock, [&] {
		return cancelled_.load(std::memory_order_acquire) || used_bytes_ + bytes <= max_bytes_;
	});
	if (cancelled_.load(std::memory_order_acquire)) {
		throw CancelledError("write budget acquisition cancelled");
	}
	used_bytes_ += bytes;
}

bool InFlightWriteBudget::try_acquire(std::size_t bytes) {
	if (bytes == 0) {
		return true;
	}
	if (bytes > max_bytes_) {
		throw std::runtime_error("requested write buffer exceeds configured in-flight write budget");
	}

	std::lock_guard lock(mutex_);
	if (cancelled_.load(std::memory_order_acquire)) {
		throw CancelledError("write budget acquisition cancelled");
	}
	if (used_bytes_ + bytes > max_bytes_) {
		return false;
	}
	used_bytes_ += bytes;
	return true;
}

void InFlightWriteBudget::release(std::size_t bytes) {
	if (bytes == 0) {
		return;
	}

	{
		std::lock_guard lock(mutex_);
		used_bytes_ -= std::min(used_bytes_, bytes);
	}
	cv_.notify_all();
}

std::size_t InFlightWriteBudget::max_bytes() const {
	return max_bytes_;
}

std::size_t InFlightWriteBudget::used_bytes() const {
	std::lock_guard lock(mutex_);
	return used_bytes_;
}

bool InFlightWriteBudget::is_cancelled() const {
	return cancelled_.load(std::memory_order_acquire);
}

DirScanner::DirScanner(
	BufferPool& pool,
	RuntimeExecutors& executors,
	std::size_t submit_concurrency,
	std::size_t buffer_size,
	CancelEvent* cancel_event)
	: pool_(pool),
	  executors_(executors),
	  submit_concurrency_(std::max<std::size_t>(1, submit_concurrency)),
	  buffer_size_(std::max<std::size_t>(1, buffer_size)),
	  cancel_event_(cancel_event) {}

boost::asio::awaitable<void> DirScanner::scan(const std::filesystem::path& root_dir, BoundedQueue<OpenedFileReader>& out_queue) const {
	throw_if_cancelled(cancel_event_);
	if (!std::filesystem::exists(root_dir)) {
		throw std::runtime_error("source directory does not exist: " + path_to_utf8_string(root_dir));
	}
	if (!std::filesystem::is_directory(root_dir)) {
		throw std::runtime_error("source path is not a directory: " + path_to_utf8_string(root_dir));
	}

	const auto archive_root_name = archive_root_name_for_directory(root_dir);

	try {
		std::map<std::size_t, std::future<OpenedFileReader>> pending_results;
		std::map<std::size_t, OpenedFileReader> ready_results;
		std::size_t next_sequence = 0;
		std::size_t next_emit_sequence = 0;

		auto emit_ready = [&](bool wait_for_next) -> boost::asio::awaitable<void> {
			while (true) {
				auto ready_it = ready_results.find(next_emit_sequence);
				if (ready_it != ready_results.end()) {
					auto opened_file = std::move(ready_it->second);
					ready_results.erase(ready_it);
					++next_emit_sequence;
					co_await out_queue.async_push_await(std::move(opened_file));
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

		auto enqueue_ready = [&](OpenedFileReader opened_file) {
			ready_results.emplace(next_sequence++, std::move(opened_file));
		};

		auto throttle_reorder_window = [&]() -> boost::asio::awaitable<void> {
			while (next_sequence - next_emit_sequence >= submit_concurrency_) {
				co_await emit_ready(true);
			}
		};

		OpenedFileReader root_entry;
		root_entry.meta.full_path = root_dir;
		populate_file_meta(root_entry.meta);
		root_entry.meta.relative_path_in_tar = archive_path_for_entry(archive_root_name, ".");
		enqueue_ready(std::move(root_entry));
		co_await emit_ready(false);

		std::deque<std::filesystem::path> directories;
		directories.push_back(root_dir);

		while (!directories.empty()) {
			throw_if_cancelled(cancel_event_);
			auto current_dir = std::move(directories.front());
			directories.pop_front();

			for (const auto& entry : std::filesystem::directory_iterator(current_dir)) {
				throw_if_cancelled(cancel_event_);
				FileMeta meta;
				meta.full_path = entry.path();
				populate_file_status(meta);
				meta.relative_path_in_tar = archive_path_for_entry(archive_root_name, entry.path().lexically_relative(root_dir));
				if (meta.relative_path_in_tar.empty()) {
					continue;
				}
				if (meta.status.type() == std::filesystem::file_type::symlink) {
					continue;
				}
				if (entry.is_directory()) {
					directories.push_back(entry.path());
				}

				if (meta.status.type() == std::filesystem::file_type::regular) {
					const auto sequence = next_sequence++;
					pending_results.emplace(
						sequence,
						executors_.post([this, meta = std::move(meta)]() mutable {
							return open_regular_file(
								pool_,
								std::move(meta),
								buffer_size_,
								cancel_event_);
						}));
				} else {
					OpenedFileReader opened_file;
					opened_file.meta = std::move(meta);
					populate_file_meta(opened_file.meta);
					ready_results.emplace(next_sequence++, std::move(opened_file));
				}

				co_await emit_ready(false);
				co_await throttle_reorder_window();
			}
		}

		while (!pending_results.empty() || !ready_results.empty()) {
			co_await emit_ready(true);
		}
	} catch (...) {
		out_queue.close();
		throw;
	}

	out_queue.close();
	co_return;
}

FileReader::FileReader(
	BufferPool& pool,
	const std::filesystem::path& path,
	std::uint64_t size,
	std::size_t buffer_size,
	HANDLE handle)
	: reader_(std::make_unique<OverlappedFileReader>(pool, path, size, buffer_size, handle)) {}

FileReader::~FileReader() = default;

FileReader::FileReader(FileReader&& other) = default;

FileReader& FileReader::operator=(FileReader&& other) {
	if (this != &other) {
		reader_ = std::move(other.reader_);
	}
	return *this;
}

void FileReader::listenCancelSignal(CancelEvent& event) {
	if (!reader_) {
		return;
	}
	reader_->listenCancelSignal(event);
}

bool FileReader::is_cancelled() const {
	return reader_ != nullptr && reader_->is_cancelled();
}

void FileReader::cancel_pending_work() {
	if (!reader_) {
		return;
	}
	reader_->cancel_pending_work();
}

void FileReader::start_prefetch(std::size_t max_bytes) {
	if (!reader_) {
		throw std::runtime_error("file reader is closed");
	}
	reader_->start_prefetch(max_bytes);
}

DataChunk FileReader::read_next_chunk() {
	if (!reader_) {
		throw std::runtime_error("file reader is closed");
	}
	return reader_->read_next_chunk();
}

std::uint64_t FileReader::offset() const {
	return reader_ == nullptr ? 0 : reader_->offset();
}

bool FileReader::eof() const {
	return reader_ == nullptr || reader_->eof();
}

bool FileReader::is_open() const {
	return reader_ != nullptr && reader_->is_open();
}

FileReaderPrefetcher::FileReaderPrefetcher(
	RuntimeExecutors& executors,
	std::shared_ptr<InFlightReadBudget> read_budget,
	std::size_t prefetch_bytes,
	CancelEvent* cancel_event)
	: executors_(executors),
	  read_budget_(std::move(read_budget)),
	  prefetch_bytes_(std::max<std::size_t>(1, prefetch_bytes)),
	  cancel_event_(cancel_event) {}

boost::asio::awaitable<void> FileReaderPrefetcher::prefetch(BoundedQueue<OpenedFileReader>& in_opened, BoundedQueue<OpenedFileReader>& out_prefetched) const {
	try {
		while (auto opened_file = co_await in_opened.async_pop_await()) {
			throw_if_cancelled(cancel_event_);
			if (opened_file->reader.has_value()) {
				const auto budget_bytes = compute_reader_prefetch_bytes(*opened_file, prefetch_bytes_);
				if (budget_bytes > 0) {
					read_budget_->acquire(budget_bytes);
					try {
						throw_if_cancelled(cancel_event_);
						opened_file->reader->start_prefetch(budget_bytes);
						opened_file->read_budget_lease = ReadBudgetLease(read_budget_, budget_bytes);
						co_await out_prefetched.async_push_await(std::move(*opened_file));
					} catch (...) {
						read_budget_->release(budget_bytes);
						throw;
					}
					continue;
				}
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

} // namespace soratransport
