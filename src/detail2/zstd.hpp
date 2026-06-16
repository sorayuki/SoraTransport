#pragma once

#include "chunk.hpp"

#include "../detail/pipeline.hpp"
#include "../detail/runtime.hpp"

#include <atomic>
#include <chrono>
#include <string>

namespace soratransport::detail2 {

// ---------------------------------------------------------------------------
// AdaptiveLevelController  — adaptive compression level regulator
// Decides level changes based on upstream/downstream queue pressure,
// with reversal annealing and cooldown to prevent oscillation.
// ---------------------------------------------------------------------------
class AdaptiveLevelController {
public:
	struct Decision {
		int level = 0;
		bool changed = false;
	};

	AdaptiveLevelController(int initial_level, int min_level, int max_level,
		std::atomic<int>* active_level = nullptr, bool log = false);

	Decision on_upstream_full(int current_level,
		std::size_t upstream_size, std::size_t upstream_capacity,
		std::size_t downstream_size, std::size_t downstream_capacity);
	Decision on_downstream_full(int current_level,
		std::size_t upstream_size, std::size_t upstream_capacity,
		std::size_t downstream_size, std::size_t downstream_capacity);
	Decision on_downstream_empty_then_upstream_full(int current_level,
		std::size_t upstream_size, std::size_t upstream_capacity,
		std::size_t downstream_size, std::size_t downstream_capacity);

private:
	enum class Direction { None, Up, Down };

	Decision evaluate(int requested_level, Direction direction, const char* reason,
		std::size_t upstream_size, std::size_t upstream_capacity,
		std::size_t downstream_size, std::size_t downstream_capacity);

	int min_level_;
	int max_level_;
	std::atomic<int>* active_level_ = nullptr;
	bool log_ = false;

	Direction last_direction_ = Direction::None;
	int last_target_level_ = 0;
	std::chrono::steady_clock::duration current_cooldown_;
	std::chrono::steady_clock::time_point last_adjustment_ = std::chrono::steady_clock::time_point::min();
};

// ---------------------------------------------------------------------------
// ZstdCompressor
// ---------------------------------------------------------------------------
class ZstdCompressor {
public:
	ZstdCompressor(
		BufferPool& pool,
		std::size_t worker_count,
		int compression_level,
		SemaphoreCor* output_budget = nullptr,
		const CompressionQueueTelemetry* queue_telemetry = nullptr,
		std::atomic<int>* active_level = nullptr,
		bool log_adaptive_decisions = false);

	void compress(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd, const CancelEvent* cancel_event = nullptr);

private:
	BufferPool& pool_;
	std::size_t worker_count_ = 1;
	int compression_level_ = 0;
	SemaphoreCor* output_budget_ = nullptr;
	const CompressionQueueTelemetry* queue_telemetry_ = nullptr;
	std::atomic<int>* active_level_ = nullptr;
	bool log_adaptive_decisions_ = false;
};

} // namespace soratransport::detail2