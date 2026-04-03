#include "pipeline.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <stdexcept>
#include <system_error>

#include <windows.h>

namespace soratransport {

namespace {

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
		std::future<std::shared_ptr<uint8_t>> prefetch_future;
		std::uint64_t offset = 0;
		std::size_t requested_length = 0;
		bool in_flight = false;
	};

	State(std::filesystem::path input_path, std::uint64_t input_size)
		: path(std::move(input_path)), size(input_size) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;
	std::uint64_t next_issue_offset = 0;
	std::size_t chunk_size = 0;
	HANDLE handle = INVALID_HANDLE_VALUE;
	HANDLE mapping_handle = nullptr;
	DWORD allocation_granularity = 0;
	std::array<ReadSlot, 4> slots;
	std::size_t next_slot_to_issue = 0;
	std::size_t next_slot_to_consume = 0;
	std::size_t in_flight_reads = 0;
};

namespace {

std::runtime_error make_win32_error(const std::string& message, DWORD error = ::GetLastError()) {
	return std::runtime_error(message + ": " + std::system_category().message(static_cast<int>(error)));
}

std::uint64_t align_down(std::uint64_t value, std::uint64_t alignment) {
	return value - (value % alignment);
}

std::shared_ptr<uint8_t> map_prefetched_chunk(
	HANDLE mapping_handle,
	const std::filesystem::path& path,
	std::uint64_t offset,
	std::size_t requested_length,
	DWORD allocation_granularity) {
	const auto view_offset = align_down(offset, allocation_granularity);
	const auto offset_within_view = static_cast<std::size_t>(offset - view_offset);
	const auto view_length = offset_within_view + requested_length;
	auto* mapped_view = static_cast<uint8_t*>(::MapViewOfFile(
		mapping_handle,
		FILE_MAP_READ,
		static_cast<DWORD>((view_offset >> 32) & 0xffffffffull),
		static_cast<DWORD>(view_offset & 0xffffffffull),
		view_length));
	if (mapped_view == nullptr) {
		throw make_win32_error("failed to map input file: " + path_to_utf8_string(path));
	}

	auto mapped_owner = std::shared_ptr<uint8_t>(mapped_view, [](uint8_t* pointer) {
		if (pointer != nullptr) {
			::UnmapViewOfFile(pointer);
		}
	});

	WIN32_MEMORY_RANGE_ENTRY range{};
	range.VirtualAddress = mapped_view + offset_within_view;
	range.NumberOfBytes = requested_length;
	if (!::PrefetchVirtualMemory(::GetCurrentProcess(), 1, &range, 0)) {
		const auto error = ::GetLastError();
		if (error != ERROR_NOT_SUPPORTED && error != ERROR_CALL_NOT_IMPLEMENTED) {
			mapped_owner.reset();
			throw make_win32_error("failed to prefetch input file: " + path_to_utf8_string(path), error);
		}
	}

	return {mapped_owner, mapped_view + offset_within_view};
}

template <typename StateT>
bool issue_next_prefetch(StateT& state, RuntimeExecutors& executors) {
	if (state.next_issue_offset >= state.size || state.in_flight_reads >= state.slots.size()) {
		return false;
	}

	auto& slot = state.slots[state.next_slot_to_issue];
	if (slot.in_flight) {
		throw std::runtime_error("file reader read slot is still in flight");
	}
	if (slot.buffer) {
		throw std::runtime_error("file reader read slot buffer is still owned by a consumer");
	}

	slot.offset = state.next_issue_offset;
	slot.requested_length = static_cast<std::size_t>(std::min<std::uint64_t>(
		state.chunk_size,
		state.size - state.next_issue_offset));
	slot.prefetch_future = executors.post_reader([
		mapping_handle = state.mapping_handle,
		path = state.path,
		offset = slot.offset,
		requested_length = slot.requested_length,
		allocation_granularity = state.allocation_granularity
	] {
		return map_prefetched_chunk(mapping_handle, path, offset, requested_length, allocation_granularity);
	});

	slot.in_flight = true;
	state.next_slot_to_issue = (state.next_slot_to_issue + 1) % state.slots.size();
	state.next_issue_offset += slot.requested_length;
	++state.in_flight_reads;
	return true;
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

FileReader::FileReader(RuntimeExecutors& executors, const std::filesystem::path& path, std::uint64_t size, std::size_t buffer_size)
	: executors_(executors), state_(std::make_unique<State>(path, size)) {
	state_->chunk_size = std::max<std::size_t>(1, buffer_size);
}

FileReader::~FileReader() {
	close();
}

FileReader::FileReader(FileReader&& other)
	: executors_(other.executors_), state_(std::move(other.state_)) {}

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

	for (auto& slot : state_->slots) {
		if (slot.in_flight && slot.prefetch_future.valid()) {
			try {
				slot.buffer = slot.prefetch_future.get();
			} catch (...) {
			}
			slot.in_flight = false;
		}
		slot.prefetch_future = {};
		slot.buffer.reset();
		slot.requested_length = 0;
		slot.offset = 0;
	}
	state_->next_slot_to_issue = 0;
	state_->next_slot_to_consume = 0;
	state_->in_flight_reads = 0;

	if (state_->mapping_handle != nullptr) {
		::CloseHandle(state_->mapping_handle);
		state_->mapping_handle = nullptr;
	}

	if (state_->handle != INVALID_HANDLE_VALUE) {
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

	state_->handle = ::CreateFileW(
		state_->path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open input file: " + path_for_error());
	}
	state_->offset = 0;
	state_->next_issue_offset = 0;
	state_->next_slot_to_issue = 0;
	state_->next_slot_to_consume = 0;
	state_->in_flight_reads = 0;
	SYSTEM_INFO system_info{};
	::GetSystemInfo(&system_info);
	state_->allocation_granularity = std::max<DWORD>(1, system_info.dwAllocationGranularity);

	if (state_->size == 0) {
		return;
	}

	state_->mapping_handle = ::CreateFileMappingW(state_->handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
	if (state_->mapping_handle == nullptr) {
		const auto error = ::GetLastError();
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to create input file mapping: " + path_for_error(), error);
	}

	while (issue_next_prefetch(*state_, executors_)) {
	}
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
		return DataChunk{std::shared_ptr<uint8_t>{}, 0, current_offset, true};
	}

	if (state_->in_flight_reads == 0) {
		issue_next_prefetch(*state_, executors_);
	}

	auto& slot = state_->slots[state_->next_slot_to_consume];
	if (!slot.in_flight) {
		throw std::runtime_error("file reader consume slot is not in flight");
	}
	try {
		slot.buffer = slot.prefetch_future.get();
	} catch (...) {
		slot.in_flight = false;
		state_->next_slot_to_consume = (state_->next_slot_to_consume + 1) % state_->slots.size();
		--state_->in_flight_reads;
		slot.requested_length = 0;
		slot.offset = 0;
		throw;
	}
	slot.in_flight = false;
	state_->next_slot_to_consume = (state_->next_slot_to_consume + 1) % state_->slots.size();
	--state_->in_flight_reads;

	const auto bytes_read = slot.requested_length;
	state_->offset = slot.offset + bytes_read;
	auto data = std::move(slot.buffer);
	auto read_offset = slot.offset;
	slot.requested_length = 0;
	slot.offset = 0;

	if (state_->offset < state_->size) {
		issue_next_prefetch(*state_, executors_);
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

FileReaderOpener::FileReaderOpener(RuntimeExecutors& executors, std::size_t open_concurrency, std::size_t buffer_size)
	: executors_(executors), open_concurrency_(std::max<std::size_t>(1, open_concurrency)), buffer_size_(std::max<std::size_t>(1, buffer_size)) {}

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
				auto reader = FileReader(executors_, opened_file.meta.full_path, opened_file.meta.size, buffer_size_);
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
