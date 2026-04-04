#include "internal.hpp"

#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;
constexpr int kMinAdaptiveCompressionLevel = 1;
constexpr int kMaxAdaptiveCompressionLevel = 12;
constexpr auto kAdaptiveAdjustmentBaseCooldown = std::chrono::seconds(1);

} // namespace

ZstdCompressor::ZstdCompressor(
	BufferPool& pool,
	RuntimeExecutors& executors,
	int compression_level,
	const CompressionQueueTelemetry* queue_telemetry,
	std::atomic<int>* active_level,
	bool log_adaptive_decisions)
	: pool_(pool),
	  executors_(executors),
	  compression_level_(compression_level),
	  queue_telemetry_(queue_telemetry),
	  active_level_(active_level),
	  log_adaptive_decisions_(log_adaptive_decisions) {}

void ZstdCompressor::compress(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd) {
	auto result = executors_.post([this, &in_tar, &out_zstd] {
		compress_sync(in_tar, out_zstd);
	});
	result.get();
}

void ZstdCompressor::compress_sync(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd) {
	auto* context = ZSTD_createCCtx();
	if (context == nullptr) {
		throw std::runtime_error("failed to create zstd compression context");
	}

	const auto output_capacity = ZSTD_CStreamOutSize();
	auto output_buffer = pool_.acquire(output_capacity);
	const bool adaptive_enabled = queue_telemetry_ != nullptr;
	bool pending_downstream_empty_check = false;
	enum class AdjustmentDirection {
		None,
		Up,
		Down,
	};
	AdjustmentDirection last_adjustment_direction = AdjustmentDirection::None;
	int last_adjustment_target_level = compression_level_;
	auto current_adjustment_cooldown = kAdaptiveAdjustmentBaseCooldown;
	auto last_adjustment_at = std::chrono::steady_clock::time_point::min();

	try {
		configure_zstd_context(context, compression_level_, executors_.thread_count());
		if (active_level_ != nullptr) {
			active_level_->store(compression_level_, std::memory_order_relaxed);
		}

		const auto apply_level = [&](int requested_level, AdjustmentDirection direction, const char* reason, std::size_t upstream_size, std::size_t upstream_capacity, std::size_t downstream_size, std::size_t downstream_capacity) {
			if (!adaptive_enabled) {
				return;
			}

			const auto now = std::chrono::steady_clock::now();
			if (last_adjustment_at != std::chrono::steady_clock::time_point::min() && now - last_adjustment_at < current_adjustment_cooldown) {
				return;
			}

			int target_level = std::clamp(requested_level, kMinAdaptiveCompressionLevel, kMaxAdaptiveCompressionLevel);
			bool reversed = false;
			if (last_adjustment_direction != AdjustmentDirection::None &&
				direction != AdjustmentDirection::None &&
				direction != last_adjustment_direction) {
				reversed = true;
				target_level = std::min(target_level, last_adjustment_target_level);
				current_adjustment_cooldown *= 2;
			} else {
				current_adjustment_cooldown = kAdaptiveAdjustmentBaseCooldown;
			}

			if (target_level == compression_level_) {
				last_adjustment_direction = direction;
				last_adjustment_target_level = target_level;
				last_adjustment_at = now;
				if (log_adaptive_decisions_) {
					std::cerr << "\n[adaptive] lvl=" << compression_level_
							  << " reason=" << reason
							  << (reversed ? "-reversed" : "")
							  << " upstream=" << upstream_size << '/' << upstream_capacity
							  << " downstream=" << downstream_size << '/' << downstream_capacity
							  << " cooldown=" << std::chrono::duration_cast<std::chrono::seconds>(current_adjustment_cooldown).count() << 's'
							  << std::flush;
				}
				return;
			}

			set_zstd_compression_level(context, target_level);
			compression_level_ = target_level;
			if (active_level_ != nullptr) {
				active_level_->store(compression_level_, std::memory_order_relaxed);
			}
			last_adjustment_direction = direction;
			last_adjustment_target_level = target_level;
			last_adjustment_at = now;
			if (log_adaptive_decisions_) {
				std::cerr << "\n[adaptive] lvl=" << compression_level_
						  << " reason=" << reason
						  << (reversed ? "-reversed" : "")
						  << " upstream=" << upstream_size << '/' << upstream_capacity
						  << " downstream=" << downstream_size << '/' << downstream_capacity
						  << " cooldown=" << std::chrono::duration_cast<std::chrono::seconds>(current_adjustment_cooldown).count() << 's'
						  << std::flush;
			}
		};

		for (;;) {
			const auto upstream_size_before_pop = in_tar.size();
			const auto upstream_capacity = in_tar.capacity();
			const bool upstream_is_full = upstream_size_before_pop >= upstream_capacity;

			if (pending_downstream_empty_check) {
				if (upstream_is_full) {
					apply_level(
						compression_level_ - 1,
						AdjustmentDirection::Down,
						"downstream-empty-then-upstream-full",
						upstream_size_before_pop,
						upstream_capacity,
						out_zstd.size(),
						out_zstd.capacity());
				}
				pending_downstream_empty_check = false;
			} else if (upstream_is_full) {
				apply_level(
					compression_level_ + 1,
					AdjustmentDirection::Up,
					"upstream-full-on-pop",
					upstream_size_before_pop,
					upstream_capacity,
					out_zstd.size(),
					out_zstd.capacity());
			}

			auto chunk = in_tar.pop();
			if (!chunk.has_value()) {
				break;
			}

			ZSTD_inBuffer input{chunk->data.get(), chunk->length, 0};
			while (input.pos < input.size) {
				ZSTD_outBuffer output{output_buffer.get(), output_capacity, 0};
				const auto result = ZSTD_compressStream2(context, &output, &input, ZSTD_e_continue);
				if (ZSTD_isError(result)) {
					throw std::runtime_error(ZSTD_getErrorName(result));
				}
				if (output.pos > 0) {
					const auto downstream_size_before_push = out_zstd.size();
					const auto downstream_capacity = out_zstd.capacity();
					if (downstream_size_before_push == 0) {
						pending_downstream_empty_check = true;
					}
					if (downstream_size_before_push >= downstream_capacity) {
						apply_level(
							compression_level_ + 1,
							AdjustmentDirection::Up,
							"downstream-full-on-push",
							in_tar.size(),
							in_tar.capacity(),
							downstream_size_before_push,
							downstream_capacity);
					}
					auto owned_output = pool_.acquire(output.pos);
					std::memcpy(owned_output.get(), output_buffer.get(), output.pos);
					out_zstd.push(DataChunk{std::move(owned_output), output.pos, 0, false});
				}
			}
		}

		for (;;) {
			ZSTD_inBuffer input{nullptr, 0, 0};
			ZSTD_outBuffer output{output_buffer.get(), output_capacity, 0};
			const auto remaining = ZSTD_compressStream2(context, &output, &input, ZSTD_e_end);
			if (ZSTD_isError(remaining)) {
				throw std::runtime_error(ZSTD_getErrorName(remaining));
			}
			if (output.pos > 0) {
				const auto downstream_size_before_push = out_zstd.size();
				const auto downstream_capacity = out_zstd.capacity();
				if (downstream_size_before_push == 0) {
					pending_downstream_empty_check = true;
				}
				if (downstream_size_before_push >= downstream_capacity) {
					apply_level(
						compression_level_ + 1,
						AdjustmentDirection::Up,
						"downstream-full-on-flush",
						in_tar.size(),
						in_tar.capacity(),
						downstream_size_before_push,
						downstream_capacity);
				}
				auto owned_output = pool_.acquire(output.pos);
				std::memcpy(owned_output.get(), output_buffer.get(), output.pos);
				out_zstd.push(DataChunk{std::move(owned_output), output.pos, 0, false});
			}
			if (remaining == 0) {
				break;
			}
		}

		out_zstd.close();
		ZSTD_freeCCtx(context);
	} catch (...) {
		out_zstd.close();
		ZSTD_freeCCtx(context);
		throw;
	}
}

ZstdDecompressor::ZstdDecompressor(BufferPool& pool) : pool_(pool) {}

void ZstdDecompressor::decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar) {
	auto* context = ZSTD_createDCtx();
	if (context == nullptr) {
		throw std::runtime_error("failed to create zstd decompression context");
	}

	auto input_storage = pool_.acquire(ZSTD_DStreamInSize());
	std::uint64_t offset = 0;
	bool eof = false;
	ZSTD_inBuffer input{input_storage.get(), 0, 0};

	try {
		while (!eof || input.pos < input.size) {
			if (input.pos == input.size && !eof) {
				input.size = source.read(input_storage.get(), ZSTD_DStreamInSize());
				input.pos = 0;
				eof = input.size == 0;
				if (eof && input.size == 0) {
					break;
				}
			}

			auto output_buffer = pool_.acquire(kPipelineChunkSize);
			ZSTD_outBuffer output{output_buffer.get(), kPipelineChunkSize, 0};
			const auto result = ZSTD_decompressStream(context, &output, &input);
			if (ZSTD_isError(result)) {
				throw std::runtime_error(ZSTD_getErrorName(result));
			}
			if (output.pos > 0) {
				out_tar.push(DataChunk{std::move(output_buffer), output.pos, offset, false});
				offset += output.pos;
			}
			if (eof && input.pos == input.size && result == 0) {
				break;
			}
		}

		out_tar.close();
		ZSTD_freeDCtx(context);
	} catch (...) {
		out_tar.close();
		ZSTD_freeDCtx(context);
		throw;
	}
}

void RawTarWriter::write(BoundedQueue<DataChunk>& in_tar, IByteSink& sink) {
	while (auto chunk = in_tar.pop()) {
		sink.write({chunk->data.get(), chunk->length});
	}
	sink.close();
}

void QueueWriter::write(BoundedQueue<DataChunk>& in_queue, IByteSink& sink) {
	while (auto chunk = in_queue.pop()) {
		sink.write({chunk->data.get(), chunk->length});
	}
	sink.close();
}

RawTarReader::RawTarReader(BufferPool& pool) : pool_(pool) {}

void RawTarReader::read(IByteSource& source, BoundedQueue<DataChunk>& out_tar) {
	std::uint64_t offset = 0;
	for (;;) {
		auto buffer = pool_.acquire(kPipelineChunkSize);
		const auto bytes_read = source.read(buffer.get(), kPipelineChunkSize);
		if (bytes_read == 0) {
			break;
		}
		out_tar.push(DataChunk{std::move(buffer), bytes_read, offset, false});
		offset += bytes_read;
	}
	out_tar.close();
}

} // namespace soratransport
