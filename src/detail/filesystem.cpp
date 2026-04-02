#include "pipeline.hpp"

#include <algorithm>
#include <fstream>
#include <map>
#include <stdexcept>

namespace soratransport {

namespace {

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
	State(std::filesystem::path input_path, std::uint64_t input_size)
		: path(std::move(input_path)), size(input_size) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;
	std::ifstream input;
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

FileReader::FileReader(BufferPool& pool, const std::filesystem::path& path, std::uint64_t size, std::size_t buffer_size)
	: pool_(pool), state_(std::make_unique<State>(path, size)), buffer_(std::max<std::size_t>(1, buffer_size)) {
	state_->input.rdbuf()->pubsetbuf(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
}

FileReader::~FileReader() {
	close();
}

FileReader::FileReader(FileReader&& other) noexcept
	: pool_(other.pool_), state_(std::move(other.state_)), buffer_(std::move(other.buffer_)) {}

FileReader& FileReader::operator=(FileReader&& other) noexcept {
	if (this != &other) {
		close();
		state_ = std::move(other.state_);
		buffer_ = std::move(other.buffer_);
	}
	return *this;
}

void FileReader::close() {
	if (!state_) {
		return;
	}

	if (state_->input.is_open()) {
		state_->input.close();
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
	if (state_->input.is_open()) {
		return;
	}

	state_->input.open(state_->path, std::ios::binary);
	if (!state_->input) {
		throw std::runtime_error("failed to open input file: " + path_for_error());
	}
	state_->offset = 0;
}

DataChunk FileReader::read_next_chunk(std::size_t length) {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (!state_->input.is_open()) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}

	const auto current_offset = state_->offset;
	const auto remaining = state_->size - current_offset;
	const auto read_length = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, length));
	auto data = pool_.acquire(read_length);
	if (read_length == 0) {
		return DataChunk{std::move(data), 0, current_offset, true};
	}

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
}

std::uint64_t FileReader::offset() const {
	return state_ == nullptr ? 0 : state_->offset;
}

bool FileReader::eof() const {
	return state_ == nullptr || state_->offset >= state_->size;
}

bool FileReader::is_open() const {
	return state_ != nullptr && state_->input.is_open();
}

FileReaderOpener::FileReaderOpener(BufferPool& pool, RuntimeExecutors& executors, std::size_t open_concurrency, std::size_t buffer_size)
	: pool_(pool), executors_(executors), open_concurrency_(std::max<std::size_t>(1, open_concurrency)), buffer_size_(std::max<std::size_t>(1, buffer_size)) {}

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
				FileReader reader(pool_, opened_file.meta.full_path, static_cast<std::uint64_t>(opened_file.meta.size), buffer_size_);
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