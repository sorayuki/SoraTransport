#pragma once

#include "../detail/types.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

namespace soratransport::detail2 {

template <typename T>
struct TaskResult {
	std::exception_ptr error;
	std::optional<T> value;
};

template <>
struct TaskResult<void> {
	std::exception_ptr error;
};

class TaskExecutor {
public:
	explicit TaskExecutor(std::size_t thread_count);
	~TaskExecutor();

	TaskExecutor(const TaskExecutor&) = delete;
	TaskExecutor& operator=(const TaskExecutor&) = delete;

	std::size_t thread_count() const;
	boost::asio::any_io_executor executor();

	template <typename Fn>
	auto submit(Fn&& fn) -> std::future<std::invoke_result_t<std::decay_t<Fn>>>;

	template <typename Fn, typename CompletionToken>
	auto async_post(Fn&& fn, CompletionToken&& token);

	template <typename Fn>
	boost::asio::awaitable<std::invoke_result_t<std::decay_t<Fn>>> post(Fn&& fn);

private:
	std::size_t thread_count_;
	boost::asio::thread_pool pool_;
};

class SemaphoreCor {
public:
	struct State;
	class Guard;

	SemaphoreCor(boost::asio::any_io_executor executor, std::size_t initial_count);
	SemaphoreCor(const SemaphoreCor&) = delete;
	SemaphoreCor& operator=(const SemaphoreCor&) = delete;

	template <typename CompletionToken>
	auto async_acquire(std::size_t permits, CompletionToken&& token);

	boost::asio::awaitable<Guard> acquire(std::size_t permits = 1);
	Guard acquire_blocking(std::size_t permits = 1);
	void cancel(std::string message = "semaphore cancelled");
	std::size_t available() const;
	bool is_cancelled() const;

	class Guard {
	public:
		Guard() = default;
		Guard(std::shared_ptr<State> state, std::size_t permits)
			: state_(std::move(state)), permits_(permits) {}
		~Guard() {
			reset();
		}

		Guard(const Guard&) = delete;
		Guard& operator=(const Guard&) = delete;

		Guard(Guard&& other) noexcept
			: state_(std::move(other.state_)), permits_(other.permits_) {
			other.permits_ = 0;
		}

		Guard& operator=(Guard&& other) noexcept {
			if (this != &other) {
				reset();
				state_ = std::move(other.state_);
				permits_ = other.permits_;
				other.permits_ = 0;
			}
			return *this;
		}

		std::size_t permits() const {
			return permits_;
		}

		void reset() {
			if (!state_ || permits_ == 0) {
				state_.reset();
				permits_ = 0;
				return;
			}
			SemaphoreCor::release(state_, permits_);
			state_.reset();
			permits_ = 0;
		}

	private:
		std::shared_ptr<State> state_;
		std::size_t permits_ = 0;
	};

	private:

	struct AcquireResult {
		std::exception_ptr error;
		std::optional<Guard> guard;
	};

	struct PendingRequest {
		std::size_t permits = 0;
		std::function<void(AcquireResult)> complete;
	};

	struct State {
		explicit State(boost::asio::any_io_executor executor_value, std::size_t initial_count)
			: executor(std::move(executor_value)), available(initial_count) {}

		boost::asio::any_io_executor executor;
		mutable std::mutex mutex;
		std::deque<PendingRequest> waiters;
		std::size_t available = 0;
		bool cancelled = false;
		std::string cancel_message = "semaphore cancelled";
	};

	static void release(const std::shared_ptr<State>& state, std::size_t permits);
	static void drain_ready_waiters_locked(State& state, std::deque<PendingRequest>& ready);

	std::shared_ptr<State> state_;
};

template <typename Fn>
auto TaskExecutor::submit(Fn&& fn) -> std::future<std::invoke_result_t<std::decay_t<Fn>>> {
	using Result = std::invoke_result_t<std::decay_t<Fn>>;
	auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Fn>(fn));
	auto future = task->get_future();
	boost::asio::post(pool_, [task]() { (*task)(); });
	return future;
}

template <typename Fn, typename CompletionToken>
auto TaskExecutor::async_post(Fn&& fn, CompletionToken&& token) {
	using Result = std::invoke_result_t<std::decay_t<Fn>>;
	return boost::asio::async_initiate<CompletionToken, void(TaskResult<Result>)>(
		[this, fn = std::forward<Fn>(fn)](auto handler) mutable {
			auto associated_executor = boost::asio::get_associated_executor(handler, executor());
			auto handler_ptr = std::make_shared<std::decay_t<decltype(handler)>>(std::move(handler));
			auto fn_ptr = std::make_shared<std::decay_t<Fn>>(std::move(fn));
			boost::asio::post(pool_, [handler_ptr, associated_executor, fn_ptr]() mutable {
				TaskResult<Result> result;
				try {
					if constexpr (std::is_void_v<Result>) {
						(*fn_ptr)();
					} else {
						result.value.emplace((*fn_ptr)());
					}
				} catch (...) {
					result.error = std::current_exception();
				}
				boost::asio::post(associated_executor, [handler_ptr, result = std::move(result)]() mutable {
					(*handler_ptr)(std::move(result));
				});
			});
		},
		token);
}

template <typename Fn>
boost::asio::awaitable<std::invoke_result_t<std::decay_t<Fn>>> TaskExecutor::post(Fn&& fn) {
	using Result = std::invoke_result_t<std::decay_t<Fn>>;
	auto result = co_await async_post(std::forward<Fn>(fn), boost::asio::use_awaitable);
	if (result.error) {
		std::rethrow_exception(result.error);
	}
	if constexpr (!std::is_void_v<Result>) {
		co_return std::move(*result.value);
	} else {
		co_return;
	}
}

template <typename CompletionToken>
auto SemaphoreCor::async_acquire(std::size_t permits, CompletionToken&& token) {
	return boost::asio::async_initiate<CompletionToken, void(AcquireResult)>(
		[state = state_, permits](auto handler) mutable {
			auto associated_executor = boost::asio::get_associated_executor(handler, state->executor);
			auto handler_ptr = std::make_shared<std::decay_t<decltype(handler)>>(std::move(handler));
			AcquireResult ready_result;
			bool complete_now = false;

			{
				std::lock_guard lock(state->mutex);
				if (state->cancelled) {
					ready_result.error = std::make_exception_ptr(CancelledError(state->cancel_message));
					complete_now = true;
				} else if (permits == 0) {
					ready_result.guard.emplace(state, 0);
					complete_now = true;
				} else if (state->waiters.empty() && state->available >= permits) {
					state->available -= permits;
					ready_result.guard.emplace(state, permits);
					complete_now = true;
				} else {
					state->waiters.push_back(PendingRequest{
						.permits = permits,
						.complete = [handler_ptr, associated_executor](AcquireResult result) mutable {
							boost::asio::post(associated_executor, [handler_ptr, result = std::move(result)]() mutable {
								(*handler_ptr)(std::move(result));
							});
						},
					});
				}
			}

			if (complete_now) {
				boost::asio::post(associated_executor, [handler_ptr, ready_result = std::move(ready_result)]() mutable {
					(*handler_ptr)(std::move(ready_result));
				});
			}
		},
		token);
}

} // namespace soratransport::detail2