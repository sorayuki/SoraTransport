#include "stream.hpp"

#include <zstd.h>

#include <stdexcept>

namespace soratransport::detail2 {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

} // namespace

// ---------------------------------------------------------------------------
// QueueWriter – drains a BoundedQueue<DataChunk> into an IByteSink
// ---------------------------------------------------------------------------

void QueueWriter::write(BoundedQueue<DataChunk>& in_queue, IByteSink& sink, const CancelEvent* cancel_event) {
	while (auto chunk = in_queue.pop()) {
		throw_if_cancelled(cancel_event);
		sink.write({chunk->data.get(), chunk->length});
	}
	sink.close();
}

// ---------------------------------------------------------------------------
// RawTarReader – reads uncompressed tar from IByteSource into a queue
// ---------------------------------------------------------------------------

RawTarReader::RawTarReader(BufferPool& pool) : pool_(&pool) {}

void RawTarReader::read(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event) {
	std::uint64_t offset = 0;
	for (;;) {
		throw_if_cancelled(cancel_event);
		auto buffer = pool_->acquire(kPipelineChunkSize);
		const auto bytes_read = source.read(buffer.get(), kPipelineChunkSize);
		if (bytes_read == 0) {
			break;
		}
		out_tar.push(DataChunk{std::move(buffer), bytes_read, offset, false});
		offset += bytes_read;
	}
	out_tar.close();
}

// ---------------------------------------------------------------------------
// ZstdDecompressor – streaming zstd decompression from IByteSource to queue
// ---------------------------------------------------------------------------

ZstdDecompressor::ZstdDecompressor(BufferPool& pool) : pool_(&pool) {}

void ZstdDecompressor::decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event) {
	auto* context = ZSTD_createDCtx();
	if (context == nullptr) {
		throw std::runtime_error("failed to create zstd decompression context");
	}

	auto input_storage = pool_->acquire(ZSTD_DStreamInSize());
	std::uint64_t offset = 0;
	bool eof = false;
	ZSTD_inBuffer input{input_storage.get(), 0, 0};

	try {
		while (!eof || input.pos < input.size) {
			throw_if_cancelled(cancel_event);
			if (input.pos == input.size && !eof) {
				input.size = source.read(input_storage.get(), ZSTD_DStreamInSize());
				input.pos = 0;
				eof = input.size == 0;
				if (eof && input.size == 0) {
					break;
				}
			}

			auto output_buffer = pool_->acquire(kPipelineChunkSize);
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

} // namespace soratransport::detail2