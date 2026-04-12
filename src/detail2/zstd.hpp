#pragma once

#include "chunk.hpp"

#include "../detail/pipeline.hpp"
#include "../detail/runtime.hpp"

#include <atomic>

namespace soratransport::detail2 {

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