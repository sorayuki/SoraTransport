#pragma once

#include "pipeline.hpp"

#include <zstd.h>

namespace soratransport {

struct RuntimeConfig {
	std::size_t scanner_threads = 1;
	std::size_t reader_threads = 1;
	std::size_t compression_threads = 1;
	std::size_t tar_queue_depth = 16;
	std::size_t read_concurrency = 4;
	std::size_t max_in_flight_read_bytes = 256 * 1024 * 1024;
};

RuntimeConfig make_runtime_config(RuntimeOptions options = {});
void configure_zstd_context(ZSTD_CCtx* context, int compression_level, std::size_t worker_count);

class PipelineState {
public:
	void fail(std::exception_ptr error);
	void rethrow_if_failed() const;

private:
	mutable std::mutex mutex_;
	std::exception_ptr error_;
};

} // namespace soratransport
