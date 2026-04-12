#include "infra.hpp"

#include <algorithm>

namespace soratransport::detail2 {

TaskExecutor::TaskExecutor(std::size_t thread_count)
	: thread_count_(std::max<std::size_t>(1, thread_count)),
	  pool_(thread_count_) {}

TaskExecutor::~TaskExecutor() {
	pool_.join();
}

std::size_t TaskExecutor::thread_count() const {
	return thread_count_;
}

boost::asio::any_io_executor TaskExecutor::executor() {
	return pool_.get_executor();
}

SemaphoreCor::SemaphoreCor(boost::asio::any_io_executor executor, std::size_t initial_count)
	: state_(std::make_shared<State>(std::move(executor), initial_count)) {}

boost::asio::awaitable<SemaphoreCor::Guard> SemaphoreCor::acquire(std::size_t permits) {
	auto result = co_await async_acquire(permits, boost::asio::use_awaitable);
	if (result.error) {
		std::rethrow_exception(result.error);
	}
	co_return std::move(*result.guard);
}

void SemaphoreCor::cancel(std::string message) {
	std::deque<PendingRequest> pending;
	{
		std::lock_guard lock(state_->mutex);
		if (state_->cancelled) {
			return;
		}
		state_->cancelled = true;
		state_->cancel_message = std::move(message);
		pending.swap(state_->waiters);
	}

	for (auto& waiter : pending) {
		AcquireResult result;
		result.error = std::make_exception_ptr(CancelledError(state_->cancel_message));
		waiter.complete(std::move(result));
	}
}

std::size_t SemaphoreCor::available() const {
	std::lock_guard lock(state_->mutex);
	return state_->available;
}

bool SemaphoreCor::is_cancelled() const {
	std::lock_guard lock(state_->mutex);
	return state_->cancelled;
}

void SemaphoreCor::release(const std::shared_ptr<State>& state, std::size_t permits) {
	if (!state || permits == 0) {
		return;
	}

	std::deque<PendingRequest> ready;
	{
		std::lock_guard lock(state->mutex);
		state->available += permits;
		drain_ready_waiters_locked(*state, ready);
	}

	for (auto& waiter : ready) {
		AcquireResult result;
		result.guard.emplace(state, waiter.permits);
		waiter.complete(std::move(result));
	}
}

void SemaphoreCor::drain_ready_waiters_locked(State& state, std::deque<PendingRequest>& ready) {
	if (state.cancelled) {
		return;
	}

	while (!state.waiters.empty()) {
		auto& next = state.waiters.front();
		if (next.permits > state.available) {
			break;
		}
		state.available -= next.permits;
		ready.push_back(std::move(next));
		state.waiters.pop_front();
	}
}

} // namespace soratransport::detail2