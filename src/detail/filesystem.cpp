#include "pipeline.hpp"
#include "win32_util.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <stdexcept>
#include <system_error>

#include <windows.h>

namespace soratransport {

namespace {

constexpr std::size_t kOverlappedReadQueueDepth = 8;
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

void populate_file_meta(FileMeta& meta) {
	meta.status = std::filesystem::symlink_status(meta.full_path);
	meta.size = 0;
	if (std::filesystem::is_regular_file(meta.status)) {
		std::error_code size_error;
		meta.size = std::filesystem::file_size(meta.full_path, size_error);
		if (size_error) {
			meta.size = 0;
		}
	}

	const auto handle = ::CreateFileW(
		meta.full_path.c_str(),
		FILE_READ_ATTRIBUTES,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
		nullptr);
	if (handle != INVALID_HANDLE_VALUE) {
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

		::CloseHandle(handle);
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

std::size_t make_direct_request_size(std::size_t preferred_size, std::size_t alignment) {
	const auto aligned_size = static_cast<std::size_t>(round_down(preferred_size, alignment));
	return std::max<std::size_t>(alignment, aligned_size);
}

class ConcurrentDirectoryWorkQueue {
public:
	void push(std::filesystem::path path) {
		{
			std::lock_guard lock(mutex_);
			queue_.push(std::move(path));
		}
		cv_.notify_one();
	}

	bool pop(std::filesystem::path& path) {
		std::unique_lock lock(mutex_);
		cv_.wait(lock, [&] { return closed_ || !queue_.empty(); });
		if (queue_.empty()) {
			return false;
		}
		path = std::move(queue_.front());
		queue_.pop();
		return true;
	}

	void close() {
		{
			std::lock_guard lock(mutex_);
			closed_ = true;
		}
		cv_.notify_all();
	}

private:
	std::queue<std::filesystem::path> queue_;
	bool closed_ = false;
	std::mutex mutex_;
	std::condition_variable cv_;
};

std::size_t compute_reader_prefetch_bytes(
	const FileMeta& meta,
	std::size_t buffer_size,
	FileIoMode io_mode) {
	if (!std::filesystem::is_regular_file(meta.status) || meta.size == 0) {
		return 0;
	}

	auto per_slot_capacity = std::max<std::size_t>(1, buffer_size);
	if (io_mode == FileIoMode::Direct) {
		per_slot_capacity = make_direct_request_size(buffer_size, query_file_io_alignment(meta.full_path).required_alignment);
	}

	return static_cast<std::size_t>(std::min<std::uint64_t>(
		meta.size,
		static_cast<std::uint64_t>(per_slot_capacity) * kOverlappedReadQueueDepth));
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

void InFlightReadBudget::acquire(std::size_t bytes) {
	if (bytes == 0) {
		return;
	}
	if (bytes > max_bytes_) {
		throw std::runtime_error("requested read buffer exceeds configured in-flight read budget");
	}

	std::unique_lock lock(mutex_);
	cv_.wait(lock, [&] { return used_bytes_ + bytes <= max_bytes_; });
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

struct FileReader::State {
	struct ReadSlot : OverlappedSlotBase {
		std::uint64_t offset = 0;
		std::size_t requested_length = 0;
	};

	State(std::filesystem::path input_path, std::uint64_t input_size, FileIoMode input_mode)
		: path(std::move(input_path)), size(input_size), io_mode(input_mode) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;
	std::uint64_t aligned_data_end = 0;
	std::uint64_t next_issue_offset = 0;
	std::size_t chunk_size = 0;
	std::size_t io_alignment = kFileIoAlignment;
	HANDLE handle = INVALID_HANDLE_VALUE;
	FileIoMode io_mode = FileIoMode::Buffered;
	bool tail_read_complete = false;
	std::array<ReadSlot, kOverlappedReadQueueDepth> slots;
	std::size_t next_slot_to_issue = 0;
	std::size_t next_slot_to_consume = 0;
	std::size_t in_flight_reads = 0;
	bool prefetch_started = false;
};

DirScanner::DirScanner(RuntimeExecutors& executors) : executors_(executors) {}

boost::asio::awaitable<void> DirScanner::scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue) const {
	if (!std::filesystem::exists(root_dir)) {
		throw std::runtime_error("source directory does not exist: " + path_to_utf8_string(root_dir));
	}
	if (!std::filesystem::is_directory(root_dir)) {
		throw std::runtime_error("source path is not a directory: " + path_to_utf8_string(root_dir));
	}

	const auto archive_root_name = archive_root_name_for_directory(root_dir);

	try {
		FileMeta root_meta;
		root_meta.full_path = root_dir;
		populate_file_meta(root_meta);
		root_meta.relative_path_in_tar = archive_path_for_entry(archive_root_name, ".");
		co_await out_queue.async_push_await(std::move(root_meta));

		std::deque<std::filesystem::path> directories;
		directories.push_back(root_dir);

		while (!directories.empty()) {
			auto current_dir = std::move(directories.front());
			directories.pop_front();

			for (const auto& entry : std::filesystem::directory_iterator(current_dir)) {
				FileMeta meta;
				meta.full_path = entry.path();
				populate_file_meta(meta);
				meta.relative_path_in_tar = archive_path_for_entry(archive_root_name, entry.path().lexically_relative(root_dir));
				if (meta.relative_path_in_tar.empty()) {
					continue;
				}
				if (meta.status.type() == std::filesystem::file_type::symlink) {
					continue;
				}
				co_await out_queue.async_push_await(std::move(meta));
				if (entry.is_directory()) {
					directories.push_back(entry.path());
				}
			}
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
	FileIoMode io_mode)
	: pool_(pool), state_(std::make_unique<State>(path, size, io_mode)) {
	state_->chunk_size = std::max<std::size_t>(1, buffer_size);
}

FileReader::~FileReader() {
	close();
}

FileReader::FileReader(FileReader&& other)
	: pool_(other.pool_), state_(std::move(other.state_)) {}

FileReader& FileReader::operator=(FileReader&& other) {
	if (this != &other) {
		close();
		state_ = std::move(other.state_);
	}
	return *this;
}

void FileReader::close() {
	if (!state_) {
		return;
	}

	if (state_->handle != INVALID_HANDLE_VALUE) {
		for (auto& slot : state_->slots) {
			if (slot.in_flight) {
				::CancelIoEx(state_->handle, &slot.overlapped);
				DWORD ignored = 0;
				::GetOverlappedResult(state_->handle, &slot.overlapped, &ignored, TRUE);
				slot.in_flight = false;
			}
			slot.buffer.reset();
			slot.requested_length = 0;
			slot.offset = 0;
		}
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}

	state_.reset();
}

std::string FileReader::path_for_error() const {
	return state_ == nullptr ? std::string() : path_to_utf8_string(state_->path);
}

bool FileReader::issue_next_read() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->next_issue_offset >= state_->aligned_data_end || state_->in_flight_reads >= state_->slots.size()) {
		return false;
	}

	const auto request_limit = static_cast<std::size_t>(std::min<std::uint64_t>(
		state_->chunk_size,
		state_->aligned_data_end - state_->next_issue_offset));
	const auto request_length = state_->io_mode == FileIoMode::Direct
		? make_direct_request_size(request_limit, state_->io_alignment)
		: request_limit;

	auto slot_index = state_->slots.size();
	for (std::size_t probe = 0; probe < state_->slots.size(); ++probe) {
		const auto index = (state_->next_slot_to_issue + probe) % state_->slots.size();
		const auto& candidate = state_->slots[index];
		if (!candidate.in_flight && !candidate.buffer) {
			slot_index = index;
			break;
		}
	}
	if (slot_index == state_->slots.size()) {
		return false;
	}

	auto& slot = state_->slots[slot_index];
	slot.offset = state_->next_issue_offset;
	slot.requested_length = request_length;

	try {
		slot.buffer = state_->io_mode == FileIoMode::Direct
			? make_aligned_buffer(slot.requested_length, state_->io_alignment)
			: pool_.acquire(slot.requested_length);
	} catch (...) {
		slot.requested_length = 0;
		slot.offset = 0;
		throw;
	}

	slot.overlapped = {};
	slot.overlapped.Offset = static_cast<DWORD>(slot.offset & 0xffffffffull);
	slot.overlapped.OffsetHigh = static_cast<DWORD>((slot.offset >> 32) & 0xffffffffull);
	slot.overlapped.hEvent = slot.event_handle;
	::ResetEvent(slot.event_handle);

	DWORD bytes_read = 0;
	const auto ok = ::ReadFile(
		state_->handle,
		slot.buffer.get(),
		static_cast<DWORD>(slot.requested_length),
		&bytes_read,
		&slot.overlapped);

	if (!ok) {
		const auto error = ::GetLastError();
		if (error != ERROR_IO_PENDING) {
			slot.buffer.reset();
			slot.requested_length = 0;
			slot.offset = 0;
			throw std::runtime_error("failed to read input file: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
		}
	}

	slot.in_flight = true;
	state_->next_slot_to_issue = (slot_index + 1) % state_->slots.size();
	state_->next_issue_offset += slot.requested_length;
	++state_->in_flight_reads;
	return true;
}

void FileReader::prime_prefetch_window(std::size_t max_bytes) {
	if (!state_ || state_->aligned_data_end == 0 || max_bytes == 0) {
		return;
	}

	std::size_t issued_bytes = 0;
	while (state_->in_flight_reads < state_->slots.size()) {
		const auto before = state_->next_issue_offset;
		if (!issue_next_read()) {
			break;
		}
		issued_bytes += static_cast<std::size_t>(state_->next_issue_offset - before);
		if (issued_bytes >= max_bytes) {
			break;
		}
	}
}

void FileReader::open() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->handle != INVALID_HANDLE_VALUE) {
		return;
	}
	const auto effective_io_mode = state_->io_mode == FileIoMode::Direct && state_->size >= state_->chunk_size * state_->slots.size()
		? FileIoMode::Direct
		: FileIoMode::Buffered;
	const auto flags = effective_io_mode == FileIoMode::Direct
		? FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING
		: FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_OVERLAPPED;

	state_->handle = ::CreateFileW(
		state_->path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		flags,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open input file: " + path_for_error());
	}
	state_->offset = 0;
	state_->io_mode = effective_io_mode;
	if (state_->io_mode == FileIoMode::Direct) {
		state_->io_alignment = query_file_io_alignment(state_->path).required_alignment;
	}
	state_->aligned_data_end = state_->io_mode == FileIoMode::Direct ? round_down(state_->size, state_->io_alignment) : state_->size;
	state_->next_issue_offset = 0;
	state_->next_slot_to_issue = 0;
	state_->next_slot_to_consume = 0;
	state_->in_flight_reads = 0;
	state_->tail_read_complete = false;
	state_->prefetch_started = false;
}

void FileReader::start_prefetch(std::size_t max_bytes) {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}
	if (state_->prefetch_started) {
		return;
	}
	state_->prefetch_started = true;
	prime_prefetch_window(max_bytes);
}

DataChunk FileReader::read_next_chunk() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}

	auto current_offset = state_->offset;
	if (current_offset >= state_->size) {
		return DataChunk{pool_.acquire(0), 0, current_offset, true};
	}

	if (state_->in_flight_reads == 0) {
		issue_next_read();
	}
	if (state_->in_flight_reads == 0) {
		if (state_->offset < state_->size && !state_->tail_read_complete) {
			const auto tail_request_size = state_->io_mode == FileIoMode::Direct ? state_->io_alignment : static_cast<std::size_t>(state_->size - state_->offset);
			std::shared_ptr<uint8_t> buffer;
			try {
				buffer = state_->io_mode == FileIoMode::Direct
					? make_aligned_buffer(tail_request_size, state_->io_alignment)
					: pool_.acquire(tail_request_size);
			} catch (...) {
				throw;
			}
			OVERLAPPED overlapped{};
			overlapped.hEvent = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
			if (overlapped.hEvent == nullptr) {
				throw make_win32_error("failed to create tail read event for input file: " + path_for_error());
			}
			overlapped.Offset = static_cast<DWORD>(state_->offset & 0xffffffffull);
			overlapped.OffsetHigh = static_cast<DWORD>((state_->offset >> 32) & 0xffffffffull);
			DWORD bytes_read = 0;
			const auto ok = ::ReadFile(
				state_->handle,
				buffer.get(),
				static_cast<DWORD>(tail_request_size),
				nullptr,
				&overlapped);
			if (!ok && ::GetLastError() != ERROR_IO_PENDING) {
				const auto error = ::GetLastError();
				::CloseHandle(overlapped.hEvent);
				throw std::runtime_error("failed to read input tail block: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
			}
			if (!::GetOverlappedResult(state_->handle, &overlapped, &bytes_read, TRUE)) {
				const auto error = ::GetLastError();
				::CloseHandle(overlapped.hEvent);
				throw std::runtime_error("failed to complete input tail block read: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
			}
			::CloseHandle(overlapped.hEvent);
			if (bytes_read == 0) {
				throw std::runtime_error("unexpected empty tail read from input file: " + path_for_error());
			}
			const auto read_offset = state_->offset;
			state_->offset += bytes_read;
			state_->tail_read_complete = true;
			return DataChunk{std::move(buffer), bytes_read, read_offset, true};
		}
		return DataChunk{pool_.acquire(0), 0, current_offset, true};
	}

	const auto consumed_slot_index = state_->next_slot_to_consume;
	auto& slot = state_->slots[consumed_slot_index];
	if (!slot.in_flight) {
		throw std::runtime_error("file reader consume slot is not in flight");
	}
	DWORD bytes_read = 0;
	if (!::GetOverlappedResult(state_->handle, &slot.overlapped, &bytes_read, TRUE)) {
		const auto error = ::GetLastError();
		slot.in_flight = false;
		slot.buffer.reset();
		slot.requested_length = 0;
		slot.offset = 0;
		throw std::runtime_error("failed to read input file: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
	}
	slot.in_flight = false;
	state_->next_slot_to_consume = (state_->next_slot_to_consume + 1) % state_->slots.size();
	--state_->in_flight_reads;

	if (bytes_read != slot.requested_length) {
		slot.buffer.reset();
		slot.requested_length = 0;
		slot.offset = 0;
		throw std::runtime_error("unexpected short read from input file: " + path_for_error());
	}

	state_->offset = slot.offset + bytes_read;
	auto data = std::move(slot.buffer);
	auto read_offset = slot.offset;
	slot.requested_length = 0;
	slot.offset = 0;

	if (state_->offset < state_->size) {
		issue_next_read();
	}

	return DataChunk{std::move(data), bytes_read, read_offset, state_->offset >= state_->size};
}

std::uint64_t FileReader::offset() const {
	return state_ == nullptr ? 0 : state_->offset;
}

bool FileReader::eof() const {
	return state_ == nullptr || state_->offset >= state_->size;
}

bool FileReader::is_open() const {
	return state_ != nullptr && state_->handle != INVALID_HANDLE_VALUE;
}

FileReaderOpener::FileReaderOpener(
	BufferPool& pool,
	RuntimeExecutors& executors,
	std::size_t submit_concurrency,
	std::size_t buffer_size,
	FileIoMode io_mode)
	: pool_(pool),
	  executors_(executors),
	  submit_concurrency_(std::max<std::size_t>(1, submit_concurrency)),
	  buffer_size_(std::max<std::size_t>(1, buffer_size)),
	  io_mode_(io_mode) {}

boost::asio::awaitable<void> FileReaderOpener::open(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const {
	try {
		open_sync(in_meta, out_opened);
		out_opened.close();
	} catch (...) {
		out_opened.close();
		throw;
	}
	co_return;
}

void FileReaderOpener::open_sync(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const {
	while (auto meta = in_meta.pop()) {
		OpenedFileReader opened_file;
		opened_file.meta = std::move(*meta);
		if (opened_file.meta.status.type() == std::filesystem::file_type::regular) {
			auto reader = FileReader(pool_, opened_file.meta.full_path, opened_file.meta.size, buffer_size_, io_mode_);
			reader.open();
			opened_file.reader.emplace(std::move(reader));
		}
		out_opened.push(std::move(opened_file));
	}
}

FileReaderPrefetcher::FileReaderPrefetcher(
	RuntimeExecutors& executors,
	std::shared_ptr<InFlightReadBudget> read_budget,
	std::size_t prefetch_bytes,
	FileIoMode io_mode)
	: executors_(executors),
	  read_budget_(std::move(read_budget)),
	  prefetch_bytes_(std::max<std::size_t>(1, prefetch_bytes)),
	  io_mode_(io_mode) {}

boost::asio::awaitable<void> FileReaderPrefetcher::prefetch(BoundedQueue<OpenedFileReader>& in_opened, BoundedQueue<OpenedFileReader>& out_prefetched) const {
	try {
		while (auto opened_file = co_await in_opened.async_pop_await()) {
			if (opened_file->reader.has_value()) {
				const auto budget_bytes = compute_reader_prefetch_bytes(opened_file->meta, prefetch_bytes_, io_mode_);
				if (budget_bytes > 0) {
					read_budget_->acquire(budget_bytes);
					try {
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
