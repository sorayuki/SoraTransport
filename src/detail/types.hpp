#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <boost/asio.hpp>

#include <coroutine>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

namespace soratransport {

struct DataChunk {
	std::shared_ptr<uint8_t> data;
	std::size_t length = 0;
	std::uint64_t offset = 0;
	bool is_eof = false;
};

struct FileTimestamp {
	std::int64_t seconds = 0;
	long nanoseconds = 0;
};

struct FileMeta {
	std::filesystem::path full_path;
	std::filesystem::file_status status;
	std::uint64_t size = 0;
	std::string relative_path_in_tar;
	std::optional<FileTimestamp> creation_time;
	std::optional<FileTimestamp> last_access_time;
	std::optional<FileTimestamp> last_write_time;
	std::optional<FileTimestamp> change_time;
	std::optional<std::uint32_t> windows_file_attributes;
	std::optional<std::string> symlink_target;
};

enum class CompressionMode {
	None,
	Zstd,
};

template <typename T>
class BoundedQueue {
public:
	explicit BoundedQueue(std::size_t capacity, boost::asio::any_io_executor executor)
		: capacity_(capacity), executor_(std::move(executor)) {}

	template <typename CompletionToken>
	auto async_push(T value, CompletionToken&& token) {
		return boost::asio::async_initiate<CompletionToken, void(bool)>(
			[this, value = std::move(value)](auto handler) mutable {
				std::unique_ptr<PendingPopBase> pending_pop;
				bool complete_now = false;
				bool was_closed = false;
				auto associated_executor = boost::asio::get_associated_executor(handler, executor_);

				{
					std::lock_guard lock(sync_mutex_);
					if (closed_) {
						was_closed = true;
						complete_now = true;
					} else if (!pending_pops_.empty()) {
						pending_pop = std::move(pending_pops_.front());
						pending_pops_.pop_front();
						complete_now = true;
					} else if (queue_.size() < capacity_) {
						queue_.push_back(std::move(value));
						complete_now = true;
						sync_not_empty_.notify_one();
					} else {
						pending_pushes_.push_back(std::make_unique<PendingPushModel<decltype(handler)>>(
							executor_,
							std::move(value),
							std::move(handler)));
						return;
					}
				}

				if (pending_pop) {
					pending_pop->complete(std::optional<T>(std::move(value)));
				}

				if (complete_now) {
					boost::asio::post(
						associated_executor,
						[handler = std::move(handler), was_closed]() mutable {
							handler(was_closed);
						});
				}
			},
			token);
	}

	template <typename CompletionToken>
	auto async_pop(CompletionToken&& token) {
		return boost::asio::async_initiate<CompletionToken, void(std::optional<T>)>(
			[this](auto handler) mutable {
				std::optional<T> immediate_value;
				bool complete_now = false;
				auto associated_executor = boost::asio::get_associated_executor(handler, executor_);

				{
					std::lock_guard lock(sync_mutex_);
					if (!queue_.empty()) {
						immediate_value = std::move(queue_.front());
						queue_.pop_front();
						promote_one_locked();
						complete_now = true;
						sync_not_full_.notify_one();
					} else if (closed_) {
						immediate_value = std::nullopt;
						complete_now = true;
					} else {
						pending_pops_.push_back(std::make_unique<PendingPopModel<decltype(handler)>>(
							executor_,
							std::move(handler)));
						return;
					}
				}

				if (complete_now) {
					boost::asio::post(
						associated_executor,
						[handler = std::move(handler), immediate_value = std::move(immediate_value)]() mutable {
							handler(std::move(immediate_value));
						});
				}
			},
			token);
	}

	boost::asio::awaitable<void> async_push_await(T value) {
		auto was_closed = co_await async_push(std::move(value), boost::asio::use_awaitable);
		if (was_closed) {
			throw std::runtime_error("queue closed");
		}
		co_return;
	}

	boost::asio::awaitable<std::optional<T>> async_pop_await() {
		auto value = co_await async_pop(boost::asio::use_awaitable);
		co_return std::move(value);
	}

	void push(T value) {
		std::unique_lock lock(sync_mutex_);
		sync_not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
		if (closed_) {
			throw std::runtime_error("queue closed");
		}
		queue_.push_back(std::move(value));
		lock.unlock();
		sync_not_empty_.notify_one();
		schedule_resume_from_sync();
	}

	std::optional<T> pop() {
		std::unique_lock lock(sync_mutex_);
		sync_not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
		if (queue_.empty()) {
			return std::nullopt;
		}
		T value = std::move(queue_.front());
		queue_.pop_front();
		lock.unlock();
		sync_not_full_.notify_one();
		schedule_resume_from_sync();
		return value;
	}

	void close() {
		std::deque<std::unique_ptr<PendingPushBase>> pending_pushes;
		std::deque<std::unique_ptr<PendingPopBase>> pending_pops;
		{
			std::lock_guard lock(sync_mutex_);
			closed_ = true;
			pending_pushes.swap(pending_pushes_);
			pending_pops.swap(pending_pops_);
		}
		sync_not_full_.notify_all();
		sync_not_empty_.notify_all();
		for (auto& waiter : pending_pushes) {
			waiter->complete(true);
		}
		for (auto& waiter : pending_pops) {
			waiter->complete(std::nullopt);
		}
	}

	std::size_t size() const {
		std::lock_guard lock(sync_mutex_);
		return queue_.size();
	}

	std::size_t capacity() const {
		return capacity_;
	}

private:
	struct PendingPushBase {
		explicit PendingPushBase(T item) : value(std::move(item)) {}
		virtual ~PendingPushBase() = default;
		virtual void complete(bool closed) = 0;

		std::optional<T> value;
	};

	template <typename Handler>
	struct PendingPushModel final : PendingPushBase {
		PendingPushModel(boost::asio::any_io_executor fallback_executor, T item, Handler&& handler)
			: PendingPushBase(std::move(item)),
			  executor(boost::asio::get_associated_executor(handler, fallback_executor)),
			  handler(std::move(handler)) {}

		void complete(bool closed) override {
			boost::asio::post(executor, [handler = std::move(handler), closed]() mutable {
				handler(closed);
			});
		}

		boost::asio::any_io_executor executor;
		Handler handler;
	};

	struct PendingPopBase {
		virtual ~PendingPopBase() = default;
		virtual void complete(std::optional<T> value) = 0;
	};

	template <typename Handler>
	struct PendingPopModel final : PendingPopBase {
		PendingPopModel(boost::asio::any_io_executor fallback_executor, Handler&& handler)
			: executor(boost::asio::get_associated_executor(handler, fallback_executor)),
			  handler(std::move(handler)) {}

		void complete(std::optional<T> value) override {
			boost::asio::post(executor, [handler = std::move(handler), value = std::move(value)]() mutable {
				handler(std::move(value));
			});
		}

		boost::asio::any_io_executor executor;
		Handler handler;
	};

	void promote_one_locked() {
		if (closed_ || pending_pushes_.empty()) {
			return;
		}

		auto push_waiter = std::move(pending_pushes_.front());
		pending_pushes_.pop_front();
		if (!pending_pops_.empty()) {
			auto pop_waiter = std::move(pending_pops_.front());
			pending_pops_.pop_front();
			pop_waiter->complete(std::optional<T>(std::move(*push_waiter->value)));
			push_waiter->complete(false);
			return;
		}

		if (queue_.size() < capacity_) {
			queue_.push_back(std::move(*push_waiter->value));
			sync_not_empty_.notify_one();
			push_waiter->complete(false);
		} else {
			pending_pushes_.push_front(std::move(push_waiter));
		}
	}

	void schedule_resume_from_sync() {
		std::unique_ptr<PendingPopBase> pop_waiter;
		{
			std::lock_guard lock(sync_mutex_);
			if (!queue_.empty() && !pending_pops_.empty()) {
				pop_waiter = std::move(pending_pops_.front());
				pending_pops_.pop_front();
				auto value = std::move(queue_.front());
				queue_.pop_front();
				pop_waiter->complete(std::optional<T>(std::move(value)));
			}
			promote_one_locked();
		}
	}

	std::size_t capacity_;
	boost::asio::any_io_executor executor_;
	std::deque<T> queue_;
	bool closed_ = false;
	mutable std::mutex sync_mutex_;
	std::condition_variable sync_not_empty_;
	std::condition_variable sync_not_full_;
	std::deque<std::unique_ptr<PendingPushBase>> pending_pushes_;
	std::deque<std::unique_ptr<PendingPopBase>> pending_pops_;
};

} // namespace soratransport
