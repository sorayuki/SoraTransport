#pragma once

#include "pipeline.hpp"

#include <zstd.h>

namespace soratransport {

struct RuntimeConfig {
	std::size_t worker_threads = 1;
	std::size_t tar_queue_depth = 64;
	std::size_t max_in_flight_read_bytes = 256 * 1024 * 1024;
	std::size_t max_in_flight_write_ops = 4;
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

} // namespace soratransport
