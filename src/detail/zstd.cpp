#include "internal.hpp"

#include <zstd.h>

#include <algorithm>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;
constexpr std::size_t kRetuneInputBytes = 64 * 1024 * 1024;
constexpr int kMinAdaptiveCompressionLevel = 1;
constexpr int kMaxAdaptiveCompressionLevel = 9;

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

		bytes_since_last_check_ += input_bytes_since_last_check;
		if (bytes_since_last_check_ < kRetuneInputBytes) {
			return current_level_;
		}
		bytes_since_last_check_ = 0;

		const auto meta_fill = telemetry_->meta().fill_ratio();
		const auto opened_fill = telemetry_->opened().fill_ratio();
		const auto tar_fill = telemetry_->tar().fill_ratio();
		if (!initialized_) {
			meta_fill_ema_ = meta_fill;
			opened_fill_ema_ = opened_fill;
			tar_fill_ema_ = tar_fill;
			initialized_ = true;
		} else {
			update_ema(meta_fill_ema_, meta_fill);
			update_ema(opened_fill_ema_, opened_fill);
			update_ema(tar_fill_ema_, tar_fill);
		}

		if (cooldown_samples_ > 0) {
			--cooldown_samples_;
			return current_level_;
		}

		const auto upstream_fill = std::max(meta_fill_ema_, opened_fill_ema_);
		const bool compression_is_backed_up =
			tar_fill_ema_ >= 0.72 &&
			(tar_fill_ema_ >= upstream_fill + 0.10 || tar_fill_ema_ >= 0.88);
		const bool compression_has_headroom =
			tar_fill_ema_ <= 0.18 &&
			upstream_fill <= 0.35;

		if (compression_is_backed_up) {
			if (pressure_score_ > 0) {
				pressure_score_ = 0;
			}
			--pressure_score_;
		} else if (compression_has_headroom) {
			if (pressure_score_ < 0) {
				pressure_score_ = 0;
			}
			++pressure_score_;
		} else {
			if (pressure_score_ > 0) {
				--pressure_score_;
			} else if (pressure_score_ < 0) {
				++pressure_score_;
			}
		}

		if (pressure_score_ <= -2 && current_level_ > kMinAdaptiveCompressionLevel) {
			--current_level_;
			pressure_score_ = 0;
			cooldown_samples_ = 2;
		} else if (pressure_score_ >= 3 && current_level_ < kMaxAdaptiveCompressionLevel) {
			++current_level_;
			pressure_score_ = 0;
			cooldown_samples_ = 3;
		}

		return current_level_;
	}

private:
	static void update_ema(double& ema, double sample) {
		constexpr double alpha = 0.35;
		ema = ema + alpha * (sample - ema);
	}

	int current_level_;
	const CompressionQueueTelemetry* telemetry_ = nullptr;
	std::size_t bytes_since_last_check_ = 0;
	int pressure_score_ = 0;
	int cooldown_samples_ = 0;
	double meta_fill_ema_ = 0.0;
	double opened_fill_ema_ = 0.0;
	double tar_fill_ema_ = 0.0;
	bool initialized_ = false;
};

} // namespace

ZstdCompressor::ZstdCompressor(
	BufferPool& pool,
	RuntimeExecutors& executors,
	int compression_level,
	const CompressionQueueTelemetry* queue_telemetry)
	: pool_(pool),
	  executors_(executors),
	  compression_level_(compression_level),
	  queue_telemetry_(queue_telemetry) {}

void ZstdCompressor::compress(BoundedQueue<DataChunk>& in_tar, IByteSink& sink) {
	auto result = executors_.post_compression([this, &in_tar, &sink] {
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
		configure_zstd_context(context, controller.initial_level(), executors_.compression_threads());

		while (auto chunk = in_tar.pop()) {
			const auto target_level = controller.maybe_retune(chunk->length);
			if (target_level != compression_level_) {
				set_zstd_compression_level(context, target_level);
				compression_level_ = target_level;
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
