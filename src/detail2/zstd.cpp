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
constexpr auto kBaseCooldown = std::chrono::seconds(1);

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

void push_compressed_output(
	BufferPool& pool,
	std::shared_ptr<std::uint8_t>& output_buffer,
	std::size_t output_pos,
	SemaphoreCor* output_budget,
	BoundedQueue<DataChunk>& out_zstd) {
	auto owned = pool.acquire(output_pos);
	std::memcpy(owned.get(), output_buffer.get(), output_pos);
	auto budget = output_budget == nullptr
		? SemaphoreCor::Guard{}
		: output_budget->acquire_blocking(output_pos);
	out_zstd.push(make_budgeted_chunk(std::move(owned), output_pos, 0, std::move(budget)));
}

void log_decision(int level, const char* reason, bool reversed,
	std::size_t up_size, std::size_t up_cap,
	std::size_t down_size, std::size_t down_cap,
	std::chrono::steady_clock::duration cooldown) {
	std::cerr << "\n[adaptive] lvl=" << level
		<< " reason=" << reason
		<< (reversed ? "-reversed" : "")
		<< " upstream=" << up_size << '/' << up_cap
		<< " downstream=" << down_size << '/' << down_cap
		<< " cooldown=" << std::chrono::duration_cast<std::chrono::seconds>(cooldown).count() << 's'
		<< std::flush;
}

} // namespace

// ===========================================================================
// AdaptiveLevelController
// ===========================================================================

AdaptiveLevelController::AdaptiveLevelController(
	int initial_level, int min_level, int max_level,
	std::atomic<int>* active_level, bool log)
	: min_level_(min_level),
	  max_level_(max_level),
	  active_level_(active_level),
	  log_(log),
	  last_target_level_(initial_level),
	  current_cooldown_(kBaseCooldown) {}

AdaptiveLevelController::Decision
AdaptiveLevelController::on_upstream_full(int current_level,
	std::size_t up_size, std::size_t up_cap,
	std::size_t down_size, std::size_t down_cap) {
	return evaluate(current_level + 1, Direction::Up,
		"upstream-full-on-pop", up_size, up_cap, down_size, down_cap);
}

AdaptiveLevelController::Decision
AdaptiveLevelController::on_downstream_full(int current_level,
	std::size_t up_size, std::size_t up_cap,
	std::size_t down_size, std::size_t down_cap) {
	return evaluate(current_level + 1, Direction::Up,
		"downstream-full-on-push", up_size, up_cap, down_size, down_cap);
}

AdaptiveLevelController::Decision
AdaptiveLevelController::on_downstream_empty_then_upstream_full(int current_level,
	std::size_t up_size, std::size_t up_cap,
	std::size_t down_size, std::size_t down_cap) {
	return evaluate(current_level - 1, Direction::Down,
		"downstream-empty-then-upstream-full", up_size, up_cap, down_size, down_cap);
}

AdaptiveLevelController::Decision
AdaptiveLevelController::evaluate(int requested_level, Direction direction,
	const char* reason, std::size_t up_size, std::size_t up_cap,
	std::size_t down_size, std::size_t down_cap) {
	const auto now = std::chrono::steady_clock::now();

	// cooldown: skip if within the cooldown window
	if (last_adjustment_ != std::chrono::steady_clock::time_point::min() &&
		now - last_adjustment_ < current_cooldown_) {
		return Decision{requested_level, false};
	}

	int target_level = std::clamp(requested_level, min_level_, max_level_);

	// reversal annealing: if direction flipped, cap the target and double cooldown
	bool reversed = false;
	if (last_direction_ != Direction::None && direction != Direction::None &&
		direction != last_direction_) {
		reversed = true;
		target_level = std::min(target_level, last_target_level_);
		current_cooldown_ *= 2;
	} else {
		current_cooldown_ = kBaseCooldown;
	}

	// update tracking state
	last_direction_ = direction;
	last_target_level_ = target_level;
	last_adjustment_ = now;

	if (log_) {
		log_decision(target_level, reason, reversed, up_size, up_cap,
			down_size, down_cap, current_cooldown_);
	}

	if (active_level_ != nullptr) {
		active_level_->store(target_level, std::memory_order_relaxed);
	}
	return Decision{target_level, true};
}

// ===========================================================================
// ZstdCompressor
// ===========================================================================

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
	bool pending_downstream_empty = false;

	AdaptiveLevelController adaptive(
		compression_level_,
		kMinAdaptiveCompressionLevel, kMaxAdaptiveCompressionLevel,
		nullptr, log_adaptive_decisions_);

	auto apply_decision = [&](const AdaptiveLevelController::Decision& d) {
		if (!d.changed) return;
		set_zstd_compression_level(context, d.level);
		compression_level_ = d.level;
	};

	try {
		configure_zstd_context(context, compression_level_, worker_count_);
		if (active_level_ != nullptr) {
			active_level_->store(compression_level_, std::memory_order_relaxed);
		}

		// ---- main compression loop ----
		for (;;) {
			throw_if_cancelled(cancel_event);
			const auto up_size = in_tar.size();
			const auto up_cap = in_tar.capacity();

			// adaptive: detect downstream-empty-then-upstream-full
			if (pending_downstream_empty) {
				if (up_size >= up_cap) {
					apply_decision(adaptive.on_downstream_empty_then_upstream_full(
						compression_level_, up_size, up_cap,
						out_zstd.size(), out_zstd.capacity()));
				}
				pending_downstream_empty = false;
			} else if (up_size >= up_cap) {
				apply_decision(adaptive.on_upstream_full(
					compression_level_, up_size, up_cap,
					out_zstd.size(), out_zstd.capacity()));
			}

			auto chunk = in_tar.pop();
			if (!chunk.has_value()) {
				break;
			}

			// ---- compress one chunk ----
			ZSTD_inBuffer input{chunk->data.get(), chunk->length, 0};
			while (input.pos < input.size) {
				throw_if_cancelled(cancel_event);
				ZSTD_outBuffer output{output_buffer.get(), output_capacity, 0};
				const auto result = ZSTD_compressStream2(context, &output, &input, ZSTD_e_continue);
				if (ZSTD_isError(result)) {
					throw std::runtime_error(ZSTD_getErrorName(result));
				}
				if (output.pos > 0) {
					const auto down_size = out_zstd.size();
					const auto down_cap = out_zstd.capacity();
					if (down_size == 0) {
						pending_downstream_empty = true;
					}
					if (down_size >= down_cap) {
						apply_decision(adaptive.on_downstream_full(
							compression_level_, in_tar.size(), in_tar.capacity(),
							down_size, down_cap));
					}
					push_compressed_output(pool_, output_buffer, output.pos,
						output_budget_, out_zstd);
				}
			}
		}

		// ---- end-of-stream flush ----
		for (;;) {
			throw_if_cancelled(cancel_event);
			ZSTD_inBuffer input{nullptr, 0, 0};
			ZSTD_outBuffer output{output_buffer.get(), output_capacity, 0};
			const auto remaining = ZSTD_compressStream2(context, &output, &input, ZSTD_e_end);
			if (ZSTD_isError(remaining)) {
				throw std::runtime_error(ZSTD_getErrorName(remaining));
			}
			if (output.pos > 0) {
				push_compressed_output(pool_, output_buffer, output.pos,
					output_budget_, out_zstd);
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