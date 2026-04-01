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
	RuntimeExecutors(std::size_t scanner_threads, std::size_t reader_threads, std::size_t compression_threads);
	~RuntimeExecutors();

	template <typename Fn>
	auto post_reader(Fn&& fn) -> std::future<std::invoke_result_t<Fn>>;

	template <typename Fn>
	auto post_compression(Fn&& fn) -> std::future<std::invoke_result_t<Fn>>;

	std::size_t scanner_threads() const;
	std::size_t compression_threads() const;

private:
	std::size_t scanner_threads_;
	std::size_t compression_threads_;
	boost::asio::thread_pool reader_pool_;
	boost::asio::thread_pool compression_pool_;
};

template <typename Fn>
auto RuntimeExecutors::post_reader(Fn&& fn) -> std::future<std::invoke_result_t<Fn>> {
	using Result = std::invoke_result_t<Fn>;
	auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Fn>(fn));
	auto future = task->get_future();
	boost::asio::post(reader_pool_, [task] { (*task)(); });
	return future;
}

template <typename Fn>
auto RuntimeExecutors::post_compression(Fn&& fn) -> std::future<std::invoke_result_t<Fn>> {
	using Result = std::invoke_result_t<Fn>;
	auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Fn>(fn));
	auto future = task->get_future();
	boost::asio::post(compression_pool_, [task] { (*task)(); });
	return future;
}

} // namespace soratransport