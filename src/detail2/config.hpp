#pragma once

#include "../detail/pipeline.hpp"

#include <cstddef>

namespace soratransport::detail2 {

struct SlotTuning {
	std::size_t max_slots = 1;
	std::size_t max_slot_bytes = 0;
};

struct PipelineTuning {
	std::size_t worker_threads = 1;
	std::size_t pipeline_chunk_size = 4 * 1024 * 1024;
	std::size_t directory_traversal_concurrency = 48;
	std::size_t file_open_concurrency = 48;
	std::size_t file_prefetch_budget_bytes = 96 * 1024 * 1024;
	std::size_t tar_output_budget_bytes = 96 * 1024 * 1024;
	std::size_t zstd_output_budget_bytes = 96 * 1024 * 1024;
	std::size_t opened_queue_capacity = 64;
	std::size_t prefetched_queue_capacity = 64;
	std::size_t tar_queue_capacity = 48;
	std::size_t zstd_queue_capacity = 48;
	std::size_t max_in_flight_write_ops = 3;
	std::size_t max_parallel_extract_files = 48;
	int default_compression_level = 3;
	SlotTuning reader_slots{3, 4 * 1024 * 1024};
	SlotTuning writer_slots{3, 4 * 1024 * 1024};
};

PipelineTuning make_pipeline_tuning(RuntimeOptions options = {});

} // namespace soratransport::detail2