#include "pipeline.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#endif

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 1024 * 1024;

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

DirScanner::DirScanner(RuntimeExecutors& executors) : executors_(executors) {}

void DirScanner::scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue) const {
	if (!std::filesystem::exists(root_dir)) {
		throw std::runtime_error("source directory does not exist: " + root_dir.string());
	}
	if (!std::filesystem::is_directory(root_dir)) {
		throw std::runtime_error("source path is not a directory: " + root_dir.string());
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
					meta.relative_path_in_tar = entry.path().lexically_relative(root_dir).generic_string();
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

FileReader::FileReader(BufferPool& pool, RuntimeExecutors& executors) : pool_(pool), executors_(executors) {}

DataChunk FileReader::read_chunk(const std::filesystem::path& path, std::uint64_t offset, std::size_t length) const {
	auto data = pool_.acquire(length);

#ifdef _WIN32
	if (length >= kPipelineChunkSize) {
		const auto native_path = path.wstring();
		HANDLE file_handle = CreateFileW(native_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
		if (file_handle != INVALID_HANDLE_VALUE) {
			HANDLE mapping_handle = CreateFileMappingW(file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
			if (mapping_handle != nullptr) {
				SYSTEM_INFO system_info;
				GetSystemInfo(&system_info);
				const auto granularity = static_cast<std::uint64_t>(system_info.dwAllocationGranularity);
				const auto aligned_offset = (offset / granularity) * granularity;
				const auto delta = static_cast<std::size_t>(offset - aligned_offset);
				const auto mapping_length = delta + length;
				void* mapped_view = MapViewOfFile(
					mapping_handle,
					FILE_MAP_READ,
					static_cast<DWORD>(aligned_offset >> 32),
					static_cast<DWORD>(aligned_offset & 0xFFFFFFFFull),
					mapping_length);
				if (mapped_view != nullptr) {
					std::memcpy(data.get(), static_cast<const std::uint8_t*>(mapped_view) + delta, length);
					UnmapViewOfFile(mapped_view);
					CloseHandle(mapping_handle);
					CloseHandle(file_handle);
					return DataChunk{std::move(data), length, offset, false};
				}
				CloseHandle(mapping_handle);
			}
			CloseHandle(file_handle);
		}
	}
#endif

	std::ifstream input(path, std::ios::binary);
	if (!input) {
		throw std::runtime_error("failed to open input file: " + path.string());
	}
	input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
	if (!input) {
		throw std::runtime_error("failed to seek input file: " + path.string());
	}
	input.read(reinterpret_cast<char*>(data.get()), static_cast<std::streamsize>(length));
	const auto bytes_read = static_cast<std::size_t>(input.gcount());
	if (bytes_read == 0 && input.bad()) {
		throw std::runtime_error("failed to read input file: " + path.string());
	}
	return DataChunk{std::move(data), bytes_read, offset, false};
}

std::future<DataChunk> FileReader::read_chunk_async(const std::filesystem::path& path, std::uint64_t offset, std::size_t length) const {
	return executors_.post_reader([this, path, offset, length] {
		return read_chunk(path, offset, length);
	});
}

} // namespace soratransport