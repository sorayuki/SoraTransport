#include "config.hpp"

#include <algorithm>
#include <thread>

namespace soratransport::detail2 {

namespace {

std::size_t detect_worker_threads() {
	const auto detected = std::thread::hardware_concurrency();
	const auto fallback = detected == 0 ? 4u : detected;
	return std::clamp<std::size_t>(static_cast<std::size_t>(fallback), 1, 12);
}

} // namespace

PipelineTuning make_pipeline_tuning(RuntimeOptions options) {
	PipelineTuning tuning;
	tuning.worker_threads = detect_worker_threads();
	tuning.file_prefetch_budget_bytes = options.max_in_flight_read_bytes.value_or(tuning.file_prefetch_budget_bytes);
	tuning.max_in_flight_write_ops = std::max<std::size_t>(1, options.max_in_flight_write_ops.value_or(tuning.max_in_flight_write_ops));
	if (options.compression_level.has_value()) {
		tuning.default_compression_level = *options.compression_level;
	}
	tuning.max_parallel_extract_files = std::max(tuning.max_parallel_extract_files, tuning.worker_threads);
	return tuning;
}

} // namespace soratransport::detail2