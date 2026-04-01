#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <boost/asio.hpp>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

namespace soratransport {

struct DataChunk {
	std::shared_ptr<uint8_t> data;
	std::size_t length = 0;
	std::uint64_t offset = 0;
	bool is_eof = false;
};

struct FileMeta {
	std::filesystem::path full_path;
	std::filesystem::file_status status;
	std::uintmax_t size = 0;
	std::string relative_path_in_tar;
};

enum class CompressionMode {
	None,
	Zstd,
};

template <typename T>
class BoundedQueue {
public:
	explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

	void push(T value) {
		std::unique_lock lock(mutex_);
		not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
		if (closed_) {
			throw std::runtime_error("queue closed");
		}
		queue_.push(std::move(value));
		not_empty_.notify_one();
	}

	std::optional<T> pop() {
		std::unique_lock lock(mutex_);
		not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
		if (queue_.empty()) {
			return std::nullopt;
		}
		T value = std::move(queue_.front());
		queue_.pop();
		not_full_.notify_one();
		return value;
	}

	void close() {
		std::lock_guard lock(mutex_);
		closed_ = true;
		not_full_.notify_all();
		not_empty_.notify_all();
	}

private:
	std::size_t capacity_;
	std::queue<T> queue_;
	bool closed_ = false;
	std::mutex mutex_;
	std::condition_variable not_empty_;
	std::condition_variable not_full_;
};

template <typename T>
class BlockingChannel {
public:
	template <typename Executor>
	BlockingChannel(Executor&&, std::size_t capacity) : capacity_(capacity) {}

	explicit BlockingChannel(std::size_t capacity) : capacity_(capacity) {}

	template <typename Handler>
	void async_send(boost::system::error_code, T value, Handler&& handler) {
		{
			std::unique_lock lock(mutex_);
			not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
			if (closed_) {
				lock.unlock();
				handler(make_closed_error());
				return;
			}
			queue_.push(std::move(value));
		}
		not_empty_.notify_one();
		handler({});
	}

	template <typename Handler>
	void async_receive(Handler&& handler) {
		std::optional<T> value;
		{
			std::unique_lock lock(mutex_);
			not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
			if (queue_.empty()) {
				lock.unlock();
				handler(make_closed_error(), T{});
				return;
			}
			value = std::move(queue_.front());
			queue_.pop();
		}
		not_full_.notify_one();
		handler({}, std::move(*value));
	}

	void close() {
		std::lock_guard lock(mutex_);
		closed_ = true;
		not_full_.notify_all();
		not_empty_.notify_all();
	}

	bool is_open() const {
		std::lock_guard lock(mutex_);
		return !closed_;
	}

private:
	static boost::system::error_code make_closed_error() {
		return make_error_code(boost::system::errc::operation_canceled);
	}

	std::size_t capacity_;
	std::queue<T> queue_;
	bool closed_ = false;
	mutable std::mutex mutex_;
	std::condition_variable not_empty_;
	std::condition_variable not_full_;
};

using ConcurrentDataChunkChannel = BlockingChannel<DataChunk>;

} // namespace soratransport