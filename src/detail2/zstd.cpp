#include "zstd.hpp"

#include "../detail/internal.hpp"

#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <stdexcept>

namespace soratransport::detail2 {

namespace {

constexpr int kMinAdaptiveCompressionLevel = 1;
constexpr int kMaxAdaptiveCompressionLevel = 12;
constexpr auto kAdaptiveAdjustmentBaseCooldown = std::chrono::seconds(1);

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

} // namespace

ZstdCompressor::ZstdCompressor(
	BufferPool& pool,
	std::size_t worker_count,
	int compression_level,
	SemaphoreCor* output_budget,
	const CompressionQueueTelemetry* queue_telemetry,
	std::atomic<int>* active_level,
	bool log_adaptive_decisions)
	: pool_(pool),
	  worker_count_(std::max<std::size_t>(1, worker_count)),
	  compression_level_(compression_level),
	  output_budget_(output_budget),
	  queue_telemetry_(queue_telemetry),
	  active_level_(active_level),
	  log_adaptive_decisions_(log_adaptive_decisions) {}

void ZstdCompressor::compress(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd, const CancelEvent* cancel_event) {
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
		configure_zstd_context(context, compression_level_, worker_count_);
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
			if (last_adjustment_direction != AdjustmentDirection::None && direction != AdjustmentDirection::None && direction != last_adjustment_direction) {
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
			throw_if_cancelled(cancel_event);
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
				throw_if_cancelled(cancel_event);
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
					auto budget_guard = output_budget_ == nullptr
						? SemaphoreCor::Guard{}
						: output_budget_->acquire_blocking(output.pos);
					out_zstd.push(make_budgeted_chunk(std::move(owned_output), output.pos, 0, std::move(budget_guard)));
				}
			}
		}

		for (;;) {
			throw_if_cancelled(cancel_event);
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
				auto budget_guard = output_budget_ == nullptr
					? SemaphoreCor::Guard{}
					: output_budget_->acquire_blocking(output.pos);
				out_zstd.push(make_budgeted_chunk(std::move(owned_output), output.pos, 0, std::move(budget_guard)));
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

} // namespace soratransport::detail2