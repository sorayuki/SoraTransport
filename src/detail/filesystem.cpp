#include "pipeline.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#endif

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 1024 * 1024;

std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	std::string result;
	result.reserve(utf8.size());
	for (char8_t ch : utf8) {
		result.push_back(static_cast<char>(ch));
	}
	return result;
}

std::string path_to_generic_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.lexically_normal().generic_u8string();
	std::string result;
	result.reserve(utf8.size());
	for (char8_t ch : utf8) {
		result.push_back(static_cast<char>(ch));
	}
	return result;
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
	explicit State(std::filesystem::path input_path) : path(std::move(input_path)) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;

#ifdef _WIN32
	HANDLE file_handle = INVALID_HANDLE_VALUE;
	HANDLE mapping_handle = nullptr;
	void* mapped_view = nullptr;
#else
	std::ifstream input;
#endif
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

FileReader::FileReader(BufferPool& pool, const std::filesystem::path& path)
	: pool_(pool), state_(std::make_unique<State>(path)) {
	std::error_code ec;
	state_->size = std::filesystem::file_size(state_->path, ec);
	if (ec) {
		throw std::runtime_error("failed to get input file size: " + path_for_error());
	}

#ifdef _WIN32
	const auto native_path = state_->path.wstring();
	state_->file_handle = CreateFileW(native_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
	if (state_->file_handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("failed to open input file: " + path_for_error());
	}

	if (state_->size != 0) {
		state_->mapping_handle = CreateFileMappingW(state_->file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
		if (state_->mapping_handle == nullptr) {
			const auto error = GetLastError();
			close();
			throw std::runtime_error("failed to create file mapping for input file: " + path_for_error() + " (error " + std::to_string(error) + ")");
		}
	}
#else
	state_->input.open(state_->path, std::ios::binary);
	if (!state_->input) {
		throw std::runtime_error("failed to open input file: " + path_for_error());
	}
#endif
}

FileReader::~FileReader() {
	close();
}

FileReader::FileReader(FileReader&& other) noexcept
	: pool_(other.pool_), state_(std::move(other.state_)) {}

FileReader& FileReader::operator=(FileReader&& other) noexcept {
	if (this != &other) {
		close();
		state_ = std::move(other.state_);
	}
	return *this;
}

void FileReader::unmap_current_view() {
	if (!state_) {
		return;
	}

#ifdef _WIN32
	if (state_->mapped_view != nullptr) {
		UnmapViewOfFile(state_->mapped_view);
		state_->mapped_view = nullptr;
	}
#endif
}

void FileReader::close() {
	if (!state_) {
		return;
	}

	unmap_current_view();

#ifdef _WIN32
	if (state_->mapping_handle != nullptr) {
		CloseHandle(state_->mapping_handle);
		state_->mapping_handle = nullptr;
	}
	if (state_->file_handle != INVALID_HANDLE_VALUE) {
		CloseHandle(state_->file_handle);
		state_->file_handle = INVALID_HANDLE_VALUE;
	}
#else
	if (state_->input.is_open()) {
		state_->input.close();
	}
#endif

	state_.reset();
}

std::string FileReader::path_for_error() const {
	return state_ == nullptr ? std::string() : path_to_utf8_string(state_->path);
}

DataChunk FileReader::read_next_chunk(std::size_t length) {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}

	const auto current_offset = state_->offset;
	const auto remaining = state_->size - current_offset;
	const auto read_length = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, length));
	auto data = pool_.acquire(read_length);
	if (read_length == 0) {
		return DataChunk{std::move(data), 0, current_offset, true};
	}

#ifdef _WIN32
	if (state_->mapping_handle == nullptr) {
		state_->offset += read_length;
		return DataChunk{std::move(data), read_length, current_offset, state_->offset >= state_->size};
	}

	unmap_current_view();

	SYSTEM_INFO system_info;
	GetSystemInfo(&system_info);
	const auto granularity = static_cast<std::uint64_t>(system_info.dwAllocationGranularity);
	const auto aligned_offset = (current_offset / granularity) * granularity;
	const auto delta = static_cast<std::size_t>(current_offset - aligned_offset);
	const auto mapping_length = delta + read_length;
	state_->mapped_view = MapViewOfFile(
		state_->mapping_handle,
		FILE_MAP_READ,
		static_cast<DWORD>(aligned_offset >> 32),
		static_cast<DWORD>(aligned_offset & 0xFFFFFFFFull),
		mapping_length);
	if (state_->mapped_view == nullptr) {
		throw std::runtime_error("failed to map input file view: " + path_for_error());
	}

	std::memcpy(data.get(), static_cast<const std::uint8_t*>(state_->mapped_view) + delta, read_length);
	state_->offset += read_length;
	return DataChunk{std::move(data), read_length, current_offset, state_->offset >= state_->size};

#else
	state_->input.read(reinterpret_cast<char*>(data.get()), static_cast<std::streamsize>(read_length));
	const auto bytes_read = static_cast<std::size_t>(state_->input.gcount());
	if (bytes_read == 0 && state_->input.bad()) {
		throw std::runtime_error("failed to read input file: " + path_for_error());
	}
	if (bytes_read != read_length && !state_->input.eof()) {
		throw std::runtime_error("unexpected short read from input file: " + path_for_error());
	}
	state_->offset += bytes_read;
	return DataChunk{std::move(data), bytes_read, current_offset, state_->offset >= state_->size};

#endif
	}

std::uint64_t FileReader::offset() const {
	return state_ == nullptr ? 0 : state_->offset;
}

bool FileReader::eof() const {
	return state_ == nullptr || state_->offset >= state_->size;
}

} // namespace soratransport