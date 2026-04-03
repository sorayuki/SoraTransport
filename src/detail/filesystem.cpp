#include "pipeline.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <stdexcept>
#include <system_error>

#include <windows.h>

namespace soratransport {

namespace {

std::uint64_t round_down(std::uint64_t value, std::size_t alignment) {
	return value - (value % alignment);
}

std::size_t make_direct_request_size(std::size_t preferred_size) {
	const auto aligned_size = static_cast<std::size_t>(round_down(preferred_size, kFileIoAlignment));
	return std::max<std::size_t>(kFileIoAlignment, aligned_size);
}

std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	return {utf8.begin(), utf8.end()};
}

std::string path_to_generic_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.lexically_normal().generic_u8string();
	return {utf8.begin(), utf8.end()};
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

struct FileReader::State {
	struct ReadSlot {
		std::shared_ptr<uint8_t> buffer;
		OVERLAPPED overlapped{};
		HANDLE event_handle = nullptr;
		std::uint64_t offset = 0;
		std::size_t requested_length = 0;
		bool in_flight = false;

		ReadSlot() {
			event_handle = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
			if (event_handle == nullptr) {
				throw std::system_error(
					static_cast<int>(::GetLastError()),
					std::system_category(),
					"failed to create file reader event");
			}
			overlapped.hEvent = event_handle;
		}

		~ReadSlot() {
			if (event_handle != nullptr) {
				::CloseHandle(event_handle);
			}
		}

		ReadSlot(const ReadSlot&) = delete;
		ReadSlot& operator=(const ReadSlot&) = delete;

		ReadSlot(ReadSlot&& other) noexcept
			: buffer(std::move(other.buffer)),
			  overlapped(other.overlapped),
			  event_handle(other.event_handle),
			  offset(other.offset),
			  requested_length(other.requested_length),
			  in_flight(other.in_flight) {
			other.overlapped = {};
			other.event_handle = nullptr;
			other.offset = 0;
			other.requested_length = 0;
			other.in_flight = false;
			overlapped.hEvent = event_handle;
		}

		ReadSlot& operator=(ReadSlot&& other) noexcept {
			if (this != &other) {
				if (event_handle != nullptr) {
					::CloseHandle(event_handle);
				}
				buffer = std::move(other.buffer);
				overlapped = other.overlapped;
				event_handle = other.event_handle;
				offset = other.offset;
				requested_length = other.requested_length;
				in_flight = other.in_flight;

				other.overlapped = {};
				other.event_handle = nullptr;
				other.offset = 0;
				other.requested_length = 0;
				other.in_flight = false;
				overlapped.hEvent = event_handle;
			}
			return *this;
		}
	};

	State(std::filesystem::path input_path, std::uint64_t input_size, FileIoMode input_mode)
		: path(std::move(input_path)), size(input_size), io_mode(input_mode) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;
	std::uint64_t aligned_data_end = 0;
	std::uint64_t next_issue_offset = 0;
	std::size_t chunk_size = 0;
	HANDLE handle = INVALID_HANDLE_VALUE;
	FileIoMode io_mode = FileIoMode::Buffered;
	bool tail_read_complete = false;
	std::array<ReadSlot, 4> slots;
	std::size_t next_slot_to_issue = 0;
	std::size_t next_slot_to_consume = 0;
	std::size_t in_flight_reads = 0;
};

namespace {

std::runtime_error make_win32_error(const std::string& message) {
	return std::runtime_error(message + ": " + std::system_category().message(static_cast<int>(::GetLastError())));
}

} // namespace

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

FileReader::FileReader(BufferPool& pool, const std::filesystem::path& path, std::uint64_t size, std::size_t buffer_size, FileIoMode io_mode)
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

void FileReader::open() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->handle != INVALID_HANDLE_VALUE) {
		return;
	}
	const auto flags = state_->io_mode == FileIoMode::Direct
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
	state_->aligned_data_end = state_->io_mode == FileIoMode::Direct ? round_down(state_->size, kFileIoAlignment) : state_->size;
	state_->next_issue_offset = 0;
	state_->next_slot_to_issue = 0;
	state_->next_slot_to_consume = 0;
	state_->in_flight_reads = 0;
	state_->tail_read_complete = false;

	if (state_->size == 0) {
		return;
	}

	auto issue_next_read = [&]() -> bool {
		if (state_->next_issue_offset >= state_->aligned_data_end || state_->in_flight_reads >= state_->slots.size()) {
			return false;
		}

		auto& slot = state_->slots[state_->next_slot_to_issue];
		if (slot.in_flight) {
			throw std::runtime_error("file reader read slot is still in flight");
		}
		if (slot.buffer) {
			throw std::runtime_error("file reader read slot buffer is still owned by a consumer");
		}

		slot.offset = state_->next_issue_offset;
		const auto request_limit = static_cast<std::size_t>(std::min<std::uint64_t>(
			state_->chunk_size,
			state_->aligned_data_end - state_->next_issue_offset));
		slot.requested_length = state_->io_mode == FileIoMode::Direct
			? make_direct_request_size(request_limit)
			: request_limit;
		slot.buffer = pool_.acquire(slot.requested_length);
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
		state_->next_slot_to_issue = (state_->next_slot_to_issue + 1) % state_->slots.size();
		state_->next_issue_offset += slot.requested_length;
		++state_->in_flight_reads;
		return true;
	};

	while (issue_next_read()) {
	}
}

DataChunk FileReader::read_next_chunk() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}

	auto issue_next_read = [&]() -> bool {
		if (state_->next_issue_offset >= state_->aligned_data_end || state_->in_flight_reads >= state_->slots.size()) {
			return false;
		}

		auto& slot = state_->slots[state_->next_slot_to_issue];
		if (slot.in_flight) {
			throw std::runtime_error("file reader read slot is still in flight");
		}
		if (slot.buffer) {
			throw std::runtime_error("file reader read slot buffer is still owned by a consumer");
		}

		slot.offset = state_->next_issue_offset;
		const auto request_limit = static_cast<std::size_t>(std::min<std::uint64_t>(
			state_->chunk_size,
			state_->aligned_data_end - state_->next_issue_offset));
		slot.requested_length = state_->io_mode == FileIoMode::Direct
			? make_direct_request_size(request_limit)
			: request_limit;
		slot.buffer = pool_.acquire(slot.requested_length);
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
		state_->next_slot_to_issue = (state_->next_slot_to_issue + 1) % state_->slots.size();
		state_->next_issue_offset += slot.requested_length;
		++state_->in_flight_reads;
		return true;
	};

	auto current_offset = state_->offset;
	if (current_offset >= state_->size) {
		return DataChunk{pool_.acquire(0), 0, current_offset, true};
	}

	if (state_->in_flight_reads == 0) {
		issue_next_read();
	}
	if (state_->in_flight_reads == 0) {
		if (state_->offset < state_->size && !state_->tail_read_complete) {
			const auto tail_request_size = kFileIoAlignment;
			auto buffer = pool_.acquire(tail_request_size);
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

	auto& slot = state_->slots[state_->next_slot_to_consume];
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

FileReaderOpener::FileReaderOpener(BufferPool& pool, RuntimeExecutors& executors, std::size_t open_concurrency, std::size_t buffer_size, FileIoMode io_mode)
	: pool_(pool), executors_(executors), open_concurrency_(std::max<std::size_t>(1, open_concurrency)), buffer_size_(std::max<std::size_t>(1, buffer_size)), io_mode_(io_mode) {}

void FileReaderOpener::open(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const {
	std::map<std::size_t, std::future<OpenedFileReader>> pending;
	std::size_t next_submit = 0;
	std::size_t next_emit = 0;
	bool input_closed = false;

	auto submit = [&](FileMeta meta) {
		pending.emplace(next_submit++, executors_.post_reader([this, meta = std::move(meta)]() mutable {
			OpenedFileReader opened_file;
			opened_file.meta = std::move(meta);
			if (opened_file.meta.status.type() == std::filesystem::file_type::regular) {
				auto reader = FileReader(pool_, opened_file.meta.full_path, opened_file.meta.size, buffer_size_, io_mode_);
				reader.open();
				opened_file.reader.emplace(std::move(reader));
			}
			return opened_file;
		}));
	};

	try {
		while (!input_closed || !pending.empty()) {
			while (!input_closed && pending.size() < open_concurrency_) {
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
