#pragma once

#include "pipeline.hpp"

#include <zstd.h>

#include <functional>
#include <thread>
#include <vector>

namespace soratransport {

struct RuntimeConfig {
	std::size_t worker_threads = 1;
	std::size_t file_open_concurrency = 1;
	std::size_t tar_queue_depth = 48;
	std::size_t max_in_flight_read_bytes = 96 * 1024 * 1024;
	std::size_t max_in_flight_write_bytes = 96 * 1024 * 1024;
	std::size_t max_in_flight_write_ops = 3;
	std::size_t max_parallel_extract_files = 48;
};

RuntimeConfig make_runtime_config(RuntimeOptions options = {});
void configure_zstd_context(ZSTD_CCtx* context, int compression_level, std::size_t worker_count);
void set_zstd_compression_level(ZSTD_CCtx* context, int compression_level);

class PipelineState {
public:
	void fail(std::exception_ptr error);
	void rethrow_if_failed() const;

private:
	mutable std::mutex mutex_;
	std::exception_ptr error_;
};

// ---------------------------------------------------------------------------
// join_and_capture  — join jthread and capture exceptions to PipelineState
// ---------------------------------------------------------------------------
inline void join_and_capture(std::jthread& thread, PipelineState& state) {
	try {
		if (thread.joinable()) {
			thread.join();
		}
	} catch (...) {
		state.fail(std::current_exception());
	}
}

// ---------------------------------------------------------------------------
// PipelineGuard  — RAII wrapper that auto-closes registered BoundedQueues
// ---------------------------------------------------------------------------
class PipelineGuard {
public:
	PipelineGuard() = default;
	PipelineGuard(const PipelineGuard&) = delete;
	PipelineGuard& operator=(const PipelineGuard&) = delete;
	PipelineGuard(PipelineGuard&&) = default;
	PipelineGuard& operator=(PipelineGuard&&) = default;

	~PipelineGuard() {
		close_all();
	}

	template <typename T>
	void watch(BoundedQueue<T>& queue) {
		closers_.push_back([&queue] { queue.close(); });
	}

	void close_all() {
		for (auto& closer : closers_) {
			closer();
		}
		closers_.clear();
	}

	void dismiss() {
		closers_.clear();
	}

private:
	std::vector<std::function<void()>> closers_;
};

} // namespace soratransport
