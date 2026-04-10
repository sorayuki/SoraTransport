#pragma once

#ifndef NOMINMAX
#define NOMINMAX
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <boost/asio.hpp>
#include <boost/signals2.hpp>

#include <atomic>
#include <concepts>
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
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace soratransport {

class CancelledError : public std::runtime_error {
public:
	explicit CancelledError(std::string message = "operation cancelled")
		: std::runtime_error(std::move(message)) {}
};

class AbandonedError : public CancelledError {
public:
	explicit AbandonedError(std::string message = "queue abandoned")
		: CancelledError(std::move(message)) {}
};

struct IQueueDisposable {
	virtual ~IQueueDisposable() = default;
	virtual void Dispose() noexcept = 0;
};

class CancelEvent {
public:
	CancelEvent() = default;
	CancelEvent(const CancelEvent&) = delete;
	CancelEvent& operator=(const CancelEvent&) = delete;

	template <typename Slot>
	boost::signals2::scoped_connection connect(Slot&& slot) {
		boost::signals2::scoped_connection connection;
		bool invoke_now = false;
		{
			std::lock_guard lock(mutex_);
			if (cancelled_.load(std::memory_order_acquire)) {
				invoke_now = true;
			} else {
				connection = boost::signals2::scoped_connection(signal_.connect(std::forward<Slot>(slot)));
			}
		}
		if (invoke_now) {
			slot();
		}
		return connection;
	}

	void emit() {
		if (cancelled_.exchange(true, std::memory_order_acq_rel)) {
			return;
		}
		std::lock_guard lock(mutex_);
		signal_();
	}

	bool is_cancelled() const {
		return cancelled_.load(std::memory_order_acquire);
	}

private:
	std::atomic<bool> cancelled_{false};
	mutable std::mutex mutex_;
	boost::signals2::signal<void()> signal_;
};

struct DataChunk : IQueueDisposable {
	DataChunk() = default;
	DataChunk(std::shared_ptr<uint8_t> bytes, std::size_t byte_count, std::uint64_t byte_offset, bool eof)
		: data(std::move(bytes)), length(byte_count), offset(byte_offset), is_eof(eof) {}

	void Dispose() noexcept override {
		data.reset();
		length = 0;
		offset = 0;
		is_eof = false;
	}

	std::shared_ptr<uint8_t> data;
	std::size_t length = 0;
	std::uint64_t offset = 0;
	bool is_eof = false;
};

struct FileTimestamp {
	std::int64_t seconds = 0;
	long nanoseconds = 0;
};

struct FileMeta : IQueueDisposable {
	void Dispose() noexcept override {}

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
	static_assert(std::derived_from<T, IQueueDisposable>, "BoundedQueue elements must implement IQueueDisposable");

	explicit BoundedQueue(std::size_t capacity, boost::asio::any_io_executor executor)
		: capacity_(capacity), executor_(std::move(executor)) {}

	void listenCancelSignal(CancelEvent& event) {
		cancel_connection_ = event.connect([this] {
			abandon();
		});
	}

	template <typename CompletionToken>
	auto async_push(T value, CompletionToken&& token) {
		return boost::asio::async_initiate<CompletionToken, void(PushResult)>(
			[this, value = std::move(value)](auto handler) mutable {
				std::unique_ptr<PendingPopBase> pending_pop;
				bool complete_now = false;
				std::exception_ptr error;
				auto associated_executor = boost::asio::get_associated_executor(handler, executor_);

				{
					std::lock_guard lock(sync_mutex_);
					if (status_ == QueueStatus::Abandoned) {
						error = std::make_exception_ptr(AbandonedError());
						complete_now = true;
					} else if (status_ == QueueStatus::Closed) {
						error = std::make_exception_ptr(std::runtime_error("queue closed"));
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
					pending_pop->complete(PopResult{nullptr, std::optional<T>(std::move(value))});
				}

				if (complete_now) {
					boost::asio::post(
						associated_executor,
						[handler = std::move(handler), error = std::move(error)]() mutable {
							handler(PushResult{std::move(error)});
						});
				}
			},
			token);
	}

	template <typename CompletionToken>
	auto async_pop(CompletionToken&& token) {
		return boost::asio::async_initiate<CompletionToken, void(PopResult)>(
			[this](auto handler) mutable {
				std::optional<T> immediate_value;
				bool complete_now = false;
				std::exception_ptr error;
				auto associated_executor = boost::asio::get_associated_executor(handler, executor_);

				{
					std::lock_guard lock(sync_mutex_);
					if (!queue_.empty()) {
						immediate_value = std::move(queue_.front());
						queue_.pop_front();
						promote_one_locked();
						complete_now = true;
						sync_not_full_.notify_one();
					} else if (status_ == QueueStatus::Closed) {
						immediate_value = std::nullopt;
						complete_now = true;
					} else if (status_ == QueueStatus::Abandoned) {
						error = std::make_exception_ptr(AbandonedError());
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
						[handler = std::move(handler), error = std::move(error), immediate_value = std::move(immediate_value)]() mutable {
							handler(PopResult{std::move(error), std::move(immediate_value)});
						});
				}
			},
			token);
	}

	boost::asio::awaitable<void> async_push_await(T value) {
		auto result = co_await async_push(std::move(value), boost::asio::use_awaitable);
		if (result.error) {
			std::rethrow_exception(result.error);
		}
		co_return;
	}

	boost::asio::awaitable<std::optional<T>> async_pop_await() {
		auto result = co_await async_pop(boost::asio::use_awaitable);
		if (result.error) {
			std::rethrow_exception(result.error);
		}
		co_return std::move(result.value);
	}

	void push(T value) {
		std::unique_lock lock(sync_mutex_);
		sync_not_full_.wait(lock, [&] { return status_ != QueueStatus::Open || queue_.size() < capacity_; });
		if (status_ == QueueStatus::Abandoned) {
			throw AbandonedError();
		}
		if (status_ == QueueStatus::Closed) {
			throw std::runtime_error("queue closed");
		}
		queue_.push_back(std::move(value));
		lock.unlock();
		sync_not_empty_.notify_one();
		schedule_resume_from_sync();
	}

	std::optional<T> pop() {
		std::unique_lock lock(sync_mutex_);
		sync_not_empty_.wait(lock, [&] { return status_ != QueueStatus::Open || !queue_.empty(); });
		if (queue_.empty()) {
			if (status_ == QueueStatus::Abandoned) {
				throw AbandonedError();
			}
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
			if (status_ != QueueStatus::Open) {
				return;
			}
			status_ = QueueStatus::Closed;
			pending_pushes.swap(pending_pushes_);
			pending_pops.swap(pending_pops_);
		}
		sync_not_full_.notify_all();
		sync_not_empty_.notify_all();
		for (auto& waiter : pending_pushes) {
			if (waiter->value.has_value()) {
				waiter->value->Dispose();
				waiter->value.reset();
			}
			waiter->complete(PushResult{std::make_exception_ptr(std::runtime_error("queue closed"))});
		}
		for (auto& waiter : pending_pops) {
			waiter->complete(PopResult{nullptr, std::nullopt});
		}
	}

	void abandon() {
		std::deque<std::unique_ptr<PendingPushBase>> pending_pushes;
		std::deque<std::unique_ptr<PendingPopBase>> pending_pops;
		std::deque<T> abandoned_items;
		{
			std::lock_guard lock(sync_mutex_);
			if (status_ == QueueStatus::Abandoned) {
				return;
			}
			status_ = QueueStatus::Abandoned;
			pending_pushes.swap(pending_pushes_);
			pending_pops.swap(pending_pops_);
			abandoned_items.swap(queue_);
		}
		sync_not_full_.notify_all();
		sync_not_empty_.notify_all();
		for (auto& item : abandoned_items) {
			item.Dispose();
		}
		auto error = std::make_exception_ptr(AbandonedError());
		for (auto& waiter : pending_pushes) {
			if (waiter->value.has_value()) {
				waiter->value->Dispose();
				waiter->value.reset();
			}
			waiter->complete(PushResult{error});
		}
		for (auto& waiter : pending_pops) {
			waiter->complete(PopResult{error, std::nullopt});
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
	struct PushResult {
		std::exception_ptr error;
	};

	struct PopResult {
		std::exception_ptr error;
		std::optional<T> value;
	};

	enum class QueueStatus {
		Open,
		Closed,
		Abandoned,
	};

	struct PendingPushBase {
		explicit PendingPushBase(T item) : value(std::move(item)) {}
		virtual ~PendingPushBase() = default;
		virtual void complete(PushResult result) = 0;

		std::optional<T> value;
	};

	template <typename Handler>
	struct PendingPushModel final : PendingPushBase {
		PendingPushModel(boost::asio::any_io_executor fallback_executor, T item, Handler&& handler)
			: PendingPushBase(std::move(item)),
			  executor(boost::asio::get_associated_executor(handler, fallback_executor)),
			  handler(std::move(handler)) {}

		void complete(PushResult result) override {
			boost::asio::post(executor, [handler = std::move(handler), result = std::move(result)]() mutable {
				handler(std::move(result));
			});
		}

		boost::asio::any_io_executor executor;
		Handler handler;
	};

	struct PendingPopBase {
		virtual ~PendingPopBase() = default;
		virtual void complete(PopResult result) = 0;
	};

	template <typename Handler>
	struct PendingPopModel final : PendingPopBase {
		PendingPopModel(boost::asio::any_io_executor fallback_executor, Handler&& handler)
			: executor(boost::asio::get_associated_executor(handler, fallback_executor)),
			  handler(std::move(handler)) {}

		void complete(PopResult result) override {
			boost::asio::post(executor, [handler = std::move(handler), result = std::move(result)]() mutable {
				handler(std::move(result));
			});
		}

		boost::asio::any_io_executor executor;
		Handler handler;
	};

	void promote_one_locked() {
		if (status_ != QueueStatus::Open || pending_pushes_.empty()) {
			return;
		}

		auto push_waiter = std::move(pending_pushes_.front());
		pending_pushes_.pop_front();
		if (!pending_pops_.empty()) {
			auto pop_waiter = std::move(pending_pops_.front());
			pending_pops_.pop_front();
			pop_waiter->complete(PopResult{nullptr, std::optional<T>(std::move(*push_waiter->value))});
			push_waiter->complete(PushResult{nullptr});
			return;
		}

		if (queue_.size() < capacity_) {
			queue_.push_back(std::move(*push_waiter->value));
			sync_not_empty_.notify_one();
			push_waiter->complete(PushResult{nullptr});
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
				pop_waiter->complete(PopResult{nullptr, std::optional<T>(std::move(value))});
			}
			promote_one_locked();
		}
	}

	std::size_t capacity_;
	boost::asio::any_io_executor executor_;
	std::deque<T> queue_;
	QueueStatus status_ = QueueStatus::Open;
	mutable std::mutex sync_mutex_;
	std::condition_variable sync_not_empty_;
	std::condition_variable sync_not_full_;
	std::deque<std::unique_ptr<PendingPushBase>> pending_pushes_;
	std::deque<std::unique_ptr<PendingPopBase>> pending_pops_;
	boost::signals2::scoped_connection cancel_connection_;
};

} // namespace soratransport
