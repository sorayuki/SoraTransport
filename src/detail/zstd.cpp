#include "internal.hpp"

#include <zstd.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;
constexpr auto kRetuneInterval = std::chrono::milliseconds(1250);
constexpr auto kWindowDuration = std::chrono::milliseconds(3500);
constexpr auto kMinimumWindowSpan = std::chrono::milliseconds(1200);
constexpr int kMinAdaptiveCompressionLevel = 1;
constexpr int kMaxAdaptiveCompressionLevel = 12;

class WindowedCompressionStats {
public:
	struct Snapshot {
		double avg_upstream_fill = 0.0;
		double avg_tar_fill = 0.0;
		double input_mib_per_sec = 0.0;
		double throughput_baseline_mib_per_sec = 0.0;
		bool ready = false;
	};

	void push_sample(std::size_t input_bytes, double upstream_fill, double tar_fill) {
		const auto now = std::chrono::steady_clock::now();
		total_input_bytes_ += input_bytes;
		samples_.push_back(Sample{now, total_input_bytes_, upstream_fill, tar_fill});
		prune(now);
	}

	Snapshot snapshot() {
		const auto now = std::chrono::steady_clock::now();
		prune(now);
		if (samples_.size() < 2) {
			return {};
		}

		const auto span = std::chrono::duration_cast<std::chrono::milliseconds>(samples_.back().timestamp - samples_.front().timestamp);
		if (span < kMinimumWindowSpan) {
			return {};
		}

		double upstream_sum = 0.0;
		double tar_sum = 0.0;
		for (const auto& sample : samples_) {
			upstream_sum += sample.upstream_fill;
			tar_sum += sample.tar_fill;
		}

		const auto bytes_delta = samples_.back().total_input_bytes - samples_.front().total_input_bytes;
		const auto seconds = std::max(0.001, static_cast<double>(span.count()) / 1000.0);
		const auto mib_per_sec = static_cast<double>(bytes_delta) / (1024.0 * 1024.0) / seconds;
		update_throughput_baseline(mib_per_sec);

		return Snapshot{
			.avg_upstream_fill = upstream_sum / static_cast<double>(samples_.size()),
			.avg_tar_fill = tar_sum / static_cast<double>(samples_.size()),
			.input_mib_per_sec = mib_per_sec,
			.throughput_baseline_mib_per_sec = throughput_baseline_mib_per_sec_,
			.ready = true,
		};
	}

private:
	struct Sample {
		std::chrono::steady_clock::time_point timestamp;
		std::uint64_t total_input_bytes = 0;
		double upstream_fill = 0.0;
		double tar_fill = 0.0;
	};

	void prune(std::chrono::steady_clock::time_point now) {
		while (samples_.size() > 2 && now - samples_.front().timestamp > kWindowDuration) {
			samples_.pop_front();
		}
	}

	void update_throughput_baseline(double throughput_mib_per_sec) {
		if (!throughput_baseline_initialized_) {
			throughput_baseline_mib_per_sec_ = throughput_mib_per_sec;
			throughput_baseline_initialized_ = true;
			return;
		}
		if (throughput_mib_per_sec >= throughput_baseline_mib_per_sec_) {
			throughput_baseline_mib_per_sec_ = throughput_baseline_mib_per_sec_ + 0.35 * (throughput_mib_per_sec - throughput_baseline_mib_per_sec_);
			return;
		}
		throughput_baseline_mib_per_sec_ = throughput_baseline_mib_per_sec_ + 0.06 * (throughput_mib_per_sec - throughput_baseline_mib_per_sec_);
	}

	std::deque<Sample> samples_;
	std::uint64_t total_input_bytes_ = 0;
	double throughput_baseline_mib_per_sec_ = 0.0;
	bool throughput_baseline_initialized_ = false;
};

class AdaptiveCompressionController {
public:
	AdaptiveCompressionController(int initial_level, const CompressionQueueTelemetry* telemetry)
		: current_level_(std::clamp(initial_level, kMinAdaptiveCompressionLevel, kMaxAdaptiveCompressionLevel)),
		  telemetry_(telemetry) {}

	int initial_level() const {
		return current_level_;
	}

	int maybe_retune(std::size_t input_bytes_since_last_check) {
		if (telemetry_ == nullptr) {
			return current_level_;
		}

		const auto meta_fill = telemetry_->meta().fill_ratio();
		const auto opened_fill = telemetry_->opened().fill_ratio();
		const auto tar_fill = telemetry_->tar().fill_ratio();
		const auto upstream_fill = std::max(meta_fill, opened_fill);
		stats_.push_sample(input_bytes_since_last_check, upstream_fill, tar_fill);

		const auto now = std::chrono::steady_clock::now();
		if (now - last_retune_at_ < kRetuneInterval) {
			return current_level_;
		}
		last_retune_at_ = now;

		if (cooldown_samples_ > 0) {
			--cooldown_samples_;
			return current_level_;
		}

		const auto window = stats_.snapshot();
		if (!window.ready) {
			return current_level_;
		}

		const auto baseline = std::max(1.0, window.throughput_baseline_mib_per_sec);
		const auto throughput_ratio = window.input_mib_per_sec / baseline;
		const bool severe_sink_backlog =
			window.avg_tar_fill >= 0.97 &&
			window.avg_upstream_fill >= 0.82 &&
			throughput_ratio >= 0.40;
		const bool sink_is_backed_up =
			window.avg_tar_fill >= 0.84 &&
			window.avg_upstream_fill >= 0.62 &&
			throughput_ratio >= 0.28;
		const bool sink_has_headroom =
			window.avg_tar_fill <= 0.30 &&
			window.avg_upstream_fill <= 0.45;

		if (sink_is_backed_up) {
			if (pressure_score_ < 0) {
				pressure_score_ = 0;
			}
			pressure_score_ += 2;
		} else if (sink_has_headroom) {
			if (pressure_score_ > 0) {
				pressure_score_ = 0;
			}
			--pressure_score_;
		} else {
			if (pressure_score_ > 0) {
				--pressure_score_;
			} else if (pressure_score_ < 0) {
				++pressure_score_;
			}
		}

		if (severe_sink_backlog && current_level_ < kMaxAdaptiveCompressionLevel) {
			current_level_ = std::min(kMaxAdaptiveCompressionLevel, current_level_ + 2);
			pressure_score_ = 0;
			cooldown_samples_ = 2;
		} else if (pressure_score_ >= 3 && current_level_ < kMaxAdaptiveCompressionLevel) {
			++current_level_;
			pressure_score_ = 0;
			cooldown_samples_ = 2;
		} else if (pressure_score_ <= -3 && current_level_ > kMinAdaptiveCompressionLevel) {
			--current_level_;
			pressure_score_ = 0;
			cooldown_samples_ = 3;
		}

		return current_level_;
	}

private:
	int current_level_;
	const CompressionQueueTelemetry* telemetry_ = nullptr;
	WindowedCompressionStats stats_;
	int pressure_score_ = 0;
	int cooldown_samples_ = 0;
	std::chrono::steady_clock::time_point last_retune_at_{};
};

} // namespace

ZstdCompressor::ZstdCompressor(
	BufferPool& pool,
	RuntimeExecutors& executors,
	int compression_level,
	const CompressionQueueTelemetry* queue_telemetry,
	std::atomic<int>* active_level)
	: pool_(pool),
	  executors_(executors),
	  compression_level_(compression_level),
	  queue_telemetry_(queue_telemetry),
	  active_level_(active_level) {}

void ZstdCompressor::compress(BoundedQueue<DataChunk>& in_tar, IByteSink& sink) {
	auto result = executors_.post([this, &in_tar, &sink] {
		compress_sync(in_tar, sink);
	});
	result.get();
}

void ZstdCompressor::compress_sync(BoundedQueue<DataChunk>& in_tar, IByteSink& sink) {
	auto* context = ZSTD_createCCtx();
	if (context == nullptr) {
		throw std::runtime_error("failed to create zstd compression context");
	}

	const auto output_capacity = ZSTD_CStreamOutSize();
	auto output_buffer = pool_.acquire(output_capacity);
	AdaptiveCompressionController controller(compression_level_, queue_telemetry_);

	try {
		configure_zstd_context(context, controller.initial_level(), executors_.thread_count());
		compression_level_ = controller.initial_level();
		if (active_level_ != nullptr) {
			active_level_->store(compression_level_, std::memory_order_relaxed);
		}

		while (auto chunk = in_tar.pop()) {
			const auto target_level = controller.maybe_retune(chunk->length);
			if (target_level != compression_level_) {
				set_zstd_compression_level(context, target_level);
				compression_level_ = target_level;
				if (active_level_ != nullptr) {
					active_level_->store(compression_level_, std::memory_order_relaxed);
				}
			}
			ZSTD_inBuffer input{chunk->data.get(), chunk->length, 0};
			while (input.pos < input.size) {
				ZSTD_outBuffer output{output_buffer.get(), output_capacity, 0};
				const auto result = ZSTD_compressStream2(context, &output, &input, ZSTD_e_continue);
				if (ZSTD_isError(result)) {
					throw std::runtime_error(ZSTD_getErrorName(result));
				}
				if (output.pos > 0) {
					sink.write({output_buffer.get(), output.pos});
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
				sink.write({output_buffer.get(), output.pos});
			}
			if (remaining == 0) {
				break;
			}
		}

		sink.close();
		ZSTD_freeCCtx(context);
	} catch (...) {
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
