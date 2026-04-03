#include "internal.hpp"

#include <zstd.h>

#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 1024 * 1024;

} // namespace

ZstdCompressor::ZstdCompressor(BufferPool& pool, RuntimeExecutors& executors, int compression_level)
	: pool_(pool), executors_(executors), compression_level_(compression_level) {}

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

	try {
		configure_zstd_context(context, compression_level_, executors_.compression_threads());

		while (auto chunk = in_tar.pop()) {
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

	std::vector<uint8_t> input_storage(ZSTD_DStreamInSize());
	std::uint64_t offset = 0;
	bool eof = false;
	ZSTD_inBuffer input{input_storage.data(), 0, 0};

	try {
		while (!eof || input.pos < input.size) {
			if (input.pos == input.size && !eof) {
				input.size = source.read(input_storage.data(), input_storage.size());
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
