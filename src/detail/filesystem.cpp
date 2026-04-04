#include "pipeline.hpp"
#include "win32_util.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <stdexcept>

#include <windows.h>

namespace soratransport {

namespace {

constexpr std::size_t kOverlappedReadQueueDepth = 8;

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
		std::size_t reserved_slot_bytes = 0;
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
	std::size_t prefetch_budget_limit = 0;
	std::size_t reserved_slot_bytes_total = 0;
	bool prefetch_started = false;
};

DirScanner::DirScanner(RuntimeExecutors& executors) : executors_(executors) {}

void DirScanner::scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue) const {
	if (!std::filesystem::exists(root_dir)) {
		throw std::runtime_error("source directory does not exist: " + path_to_utf8_string(root_dir));
	}
	if (!std::filesystem::is_directory(root_dir)) {
		throw std::runtime_error("source path is not a directory: " + path_to_utf8_string(root_dir));
	}

	ConcurrentDirectoryWorkQueue directory_queue;
	std::atomic<std::size_t> in_flight_directories = 1;
	std::exception_ptr worker_error;
	std::mutex error_mutex;
	directory_queue.push(root_dir);

	auto worker = [&] {
		try {
			std::filesystem::path current_dir;
			while (directory_queue.pop(current_dir)) {
				for (const auto& entry : std::filesystem::directory_iterator(current_dir)) {
					FileMeta meta;
					meta.full_path = entry.path();
					meta.status = entry.symlink_status();
					meta.size = entry.is_regular_file() ? entry.file_size() : 0;
					meta.relative_path_in_tar = path_to_generic_utf8_string(entry.path().lexically_relative(root_dir));
					if (meta.relative_path_in_tar.empty()) {
						continue;
					}
					out_queue.push(meta);
					if (entry.is_directory()) {
						in_flight_directories.fetch_add(1, std::memory_order_relaxed);
						directory_queue.push(entry.path());
					}
				}

				if (in_flight_directories.fetch_sub(1, std::memory_order_acq_rel) == 1) {
					directory_queue.close();
				}
			}
		} catch (...) {
			{
				std::lock_guard lock(error_mutex);
				if (!worker_error) {
					worker_error = std::current_exception();
				}
			}
			directory_queue.close();
		}
	};

	std::vector<std::jthread> workers;
	workers.reserve(executors_.scanner_threads());
	for (std::size_t index = 0; index < executors_.scanner_threads(); ++index) {
		workers.emplace_back(worker);
	}

	for (auto& thread : workers) {
		if (thread.joinable()) {
			thread.join();
		}
	}

	if (worker_error) {
		out_queue.close();
		std::rethrow_exception(worker_error);
	}

	if (std::filesystem::is_empty(root_dir)) {
		FileMeta meta;
		meta.full_path = root_dir;
		meta.status = std::filesystem::status(root_dir);
		meta.relative_path_in_tar = ".";
		out_queue.push(std::move(meta));
	}
	out_queue.close();
}

FileReader::FileReader(
	BufferPool& pool,
	std::shared_ptr<InFlightReadBudget> read_budget,
	const std::filesystem::path& path,
	std::uint64_t size,
	std::size_t buffer_size,
	FileIoMode io_mode)
	: pool_(pool), read_budget_(std::move(read_budget)), state_(std::make_unique<State>(path, size, io_mode)) {
	state_->chunk_size = std::max<std::size_t>(1, buffer_size);
}

FileReader::~FileReader() {
	close();
}

FileReader::FileReader(FileReader&& other)
	: pool_(other.pool_), read_budget_(std::move(other.read_budget_)), state_(std::move(other.state_)) {}

FileReader& FileReader::operator=(FileReader&& other) {
	if (this != &other) {
		close();
		read_budget_ = std::move(other.read_budget_);
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
			slot.reserved_slot_bytes = 0;
		}
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}
	if (read_budget_ && state_->reserved_slot_bytes_total != 0) {
		read_budget_->release(state_->reserved_slot_bytes_total);
	}
	state_->prefetch_budget_limit = 0;
	state_->reserved_slot_bytes_total = 0;

	state_.reset();
}

std::string FileReader::path_for_error() const {
	return state_ == nullptr ? std::string() : path_to_utf8_string(state_->path);
}

bool FileReader::issue_next_read(bool wait_for_budget) {
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

	auto choose_slot = [&](bool require_reserved_budget) -> std::size_t {
		for (std::size_t probe = 0; probe < state_->slots.size(); ++probe) {
			const auto index = (state_->next_slot_to_issue + probe) % state_->slots.size();
			const auto& candidate = state_->slots[index];
			if (candidate.in_flight || candidate.buffer) {
				continue;
			}
			if (require_reserved_budget && candidate.reserved_slot_bytes < request_length) {
				continue;
			}
			return index;
		}
		return state_->slots.size();
	};

	auto slot_index = choose_slot(true);
	if (slot_index == state_->slots.size()) {
		slot_index = choose_slot(false);
	}
	if (slot_index == state_->slots.size()) {
		return false;
	}

	auto& slot = state_->slots[slot_index];
	slot.offset = state_->next_issue_offset;
	slot.requested_length = request_length;
	if (wait_for_budget) {
		if (slot.reserved_slot_bytes == 0) {
			if (state_->reserved_slot_bytes_total + slot.requested_length > state_->prefetch_budget_limit) {
				slot.requested_length = 0;
				slot.offset = 0;
				return false;
			}
			if (read_budget_) {
				if (!read_budget_->try_acquire(slot.requested_length)) {
					slot.requested_length = 0;
					slot.offset = 0;
					return false;
				}
			}
			slot.reserved_slot_bytes = slot.requested_length;
			state_->reserved_slot_bytes_total += slot.requested_length;
		}
	}

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

void FileReader::prime_prefetch_window() {
	if (!state_ || state_->aligned_data_end == 0 || state_->prefetch_budget_limit == 0) {
		return;
	}

	while (issue_next_read(true)) {
	}
}

void FileReader::release_slot_budget(std::size_t slot_index) {
	if (!state_) {
		return;
	}

	auto& slot = state_->slots[slot_index];
	if (slot.reserved_slot_bytes == 0) {
		return;
	}

	if (read_budget_) {
		read_budget_->release(slot.reserved_slot_bytes);
	}
	state_->reserved_slot_bytes_total -= std::min(state_->reserved_slot_bytes_total, slot.reserved_slot_bytes);
	slot.reserved_slot_bytes = 0;
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

	if (state_->size == 0) {
		return;
	}

}

void FileReader::reserve_prefetch_budget(std::size_t bytes) {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->prefetch_started) {
		throw std::runtime_error("cannot reserve prefetch budget after prefetch started");
	}
	if (state_->prefetch_budget_limit != 0) {
		throw std::runtime_error("prefetch budget already reserved for file reader");
	}
	if (bytes == 0) {
		return;
	}
	state_->prefetch_budget_limit = bytes;
}

void FileReader::start_prefetch() {
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
	prime_prefetch_window();
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
		issue_next_read(false);
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
	release_slot_budget(consumed_slot_index);

	if (state_->offset < state_->size) {
		issue_next_read(false);
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
	std::shared_ptr<InFlightReadBudget> read_budget,
	std::size_t buffer_size,
	FileIoMode io_mode)
	: pool_(pool),
	  executors_(executors),
	  submit_concurrency_(std::max<std::size_t>(1, submit_concurrency)),
	  read_budget_(std::move(read_budget)),
	  buffer_size_(std::max<std::size_t>(1, buffer_size)),
	  io_mode_(io_mode) {}

void FileReaderOpener::open(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const {
	std::map<std::size_t, std::future<OpenedFileReader>> pending;
	std::size_t next_submit = 0;
	std::size_t next_emit = 0;
	bool input_closed = false;

	auto compute_prefetch_reservation = [&](const OpenedFileReader& opened_file) -> std::size_t {
		if (!opened_file.reader.has_value()) {
			return 0;
		}
		if (opened_file.meta.size == 0) {
			return 0;
		}

		std::size_t per_slot_capacity = buffer_size_;
		if (io_mode_ == FileIoMode::Direct) {
			const auto alignment = query_file_io_alignment(opened_file.meta.full_path).required_alignment;
			per_slot_capacity = make_direct_request_size(buffer_size_, alignment);
			return static_cast<std::size_t>(std::min<std::uint64_t>(
				opened_file.meta.size,
				static_cast<std::uint64_t>(per_slot_capacity) * kOverlappedReadQueueDepth));
		}

		return static_cast<std::size_t>(std::min<std::uint64_t>(
			opened_file.meta.size,
			static_cast<std::uint64_t>(per_slot_capacity) * kOverlappedReadQueueDepth));
	};

	auto submit = [&](FileMeta meta) {
		pending.emplace(next_submit++, executors_.post_reader([this, meta = std::move(meta)]() mutable {
			OpenedFileReader opened_file;
			opened_file.meta = std::move(meta);
			if (opened_file.meta.status.type() == std::filesystem::file_type::regular) {
				auto reader = FileReader(pool_, read_budget_, opened_file.meta.full_path, opened_file.meta.size, buffer_size_, io_mode_);
				opened_file.reader.emplace(std::move(reader));
			}
			return opened_file;
		}));
	};

	try {
		while (!input_closed || !pending.empty()) {
			while (!input_closed && pending.size() < submit_concurrency_) {
				auto meta = in_meta.pop();
				if (!meta) {
					input_closed = true;
					break;
				}
				submit(std::move(*meta));
			}

			auto current = pending.find(next_emit);
			if (current == pending.end()) {
				continue;
			}

			auto opened_file = current->second.get();
			pending.erase(current);
			if (opened_file.reader.has_value()) {
				auto reservation_bytes = compute_prefetch_reservation(opened_file);
				opened_file.reader->open();
				opened_file.reader->reserve_prefetch_budget(reservation_bytes);
				opened_file.reader->start_prefetch();
			}
			out_opened.push(std::move(opened_file));
			++next_emit;
		}
		out_opened.close();
	} catch (...) {
		out_opened.close();
		throw;
	}
}

} // namespace soratransport
