#include "config.hpp"

#include <algorithm>
#include <thread>

namespace soratransport::detail2 {

namespace {

constexpr std::size_t kTargetInFlightReadBytes = 96 * 1024 * 1024;
constexpr std::size_t kDefaultTarQueueCapacity = 48;
constexpr std::size_t kDefaultOpenedQueueCapacity = 64;
constexpr std::size_t kDefaultMaxParallelExtractFiles = 48;
constexpr std::size_t kOpenConcurrencyMultiplier = 4;
constexpr std::size_t kMaxDefaultOpenConcurrency = 48;

std::size_t detect_worker_threads() {
	const auto detected = std::thread::hardware_concurrency();
	const auto fallback = detected == 0 ? 4u : detected;
	return std::clamp<std::size_t>(static_cast<std::size_t>(fallback), 1, 12);
}

std::size_t detect_open_concurrency(std::size_t worker_threads) {
	return std::clamp<std::size_t>(
		worker_threads * kOpenConcurrencyMultiplier,
		static_cast<std::size_t>(1),
		kMaxDefaultOpenConcurrency);
}

} // namespace

PipelineTuning make_pipeline_tuning(RuntimeOptions options) {
	PipelineTuning tuning;
	tuning.worker_threads = detect_worker_threads();
	tuning.file_open_concurrency = detect_open_concurrency(tuning.worker_threads);
	tuning.directory_traversal_concurrency = tuning.file_open_concurrency;
	tuning.opened_queue_capacity = std::max(kDefaultOpenedQueueCapacity, tuning.file_open_concurrency);
	tuning.tar_queue_capacity = kDefaultTarQueueCapacity;
	tuning.zstd_queue_capacity = tuning.tar_queue_capacity;
	tuning.file_prefetch_budget_bytes = options.max_in_flight_read_bytes.value_or(kTargetInFlightReadBytes);
	// Keep the byte budgets at least as large as the queue window so queue depth, not budget, is the first limiter.
	tuning.tar_output_budget_bytes = std::max(tuning.tar_output_budget_bytes, tuning.tar_queue_capacity * tuning.pipeline_chunk_size);
	tuning.zstd_output_budget_bytes = std::max(tuning.zstd_output_budget_bytes, tuning.zstd_queue_capacity * tuning.pipeline_chunk_size);
	tuning.max_in_flight_write_ops = std::max<std::size_t>(1, options.max_in_flight_write_ops.value_or(tuning.max_in_flight_write_ops));
	if (options.compression_level.has_value()) {
		tuning.default_compression_level = *options.compression_level;
	}
	tuning.max_parallel_extract_files = std::max(kDefaultMaxParallelExtractFiles, tuning.worker_threads);
	return tuning;
}

} // namespace soratransport::detail2