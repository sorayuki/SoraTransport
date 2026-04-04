#pragma once

#include "types.hpp"

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>

#include <future>
#include <map>
#include <unordered_map>
#include <vector>

namespace soratransport {

class BufferPool {
public:
	BufferPool();
	~BufferPool();

	std::shared_ptr<uint8_t> acquire(std::size_t requested_size);
	std::size_t bucket_for(std::size_t requested_size) const;

private:
	void recycle(std::size_t bucket_size, uint8_t* buffer);

	std::vector<std::size_t> buckets_;
	std::unordered_map<std::size_t, std::vector<uint8_t*>> free_lists_;
	mutable std::mutex mutex_;
};

class RuntimeExecutors {
public:
	explicit RuntimeExecutors(std::size_t thread_count);
	~RuntimeExecutors();

	template <typename Fn>
	auto post(Fn&& fn) -> std::future<std::invoke_result_t<Fn>>;

	std::size_t thread_count() const;
	boost::asio::any_io_executor executor();

private:
	std::size_t thread_count_;
	boost::asio::thread_pool pool_;
};

template <typename Fn>
auto RuntimeExecutors::post(Fn&& fn) -> std::future<std::invoke_result_t<Fn>> {
	using Result = std::invoke_result_t<Fn>;
	auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Fn>(fn));
	auto future = task->get_future();
	boost::asio::post(pool_, [task] { (*task)(); });
	return future;
}

} // namespace soratransport
