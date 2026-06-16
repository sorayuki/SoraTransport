#pragma once

#include "../detail/pipeline.hpp"
#include "../detail/runtime.hpp"

namespace soratransport::detail2 {

class QueueWriter {
public:
	void write(BoundedQueue<DataChunk>& in_queue, IByteSink& sink, const CancelEvent* cancel_event = nullptr);
};

class RawTarReader {
public:
	explicit RawTarReader(BufferPool& pool);
	void read(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event = nullptr);

private:
	BufferPool* pool_ = nullptr;
};

class ZstdDecompressor {
public:
	explicit ZstdDecompressor(BufferPool& pool);
	void decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event = nullptr);

private:
	BufferPool* pool_ = nullptr;
};

// TarUnpacker has moved to detail2/tar.hpp (native implementation using BufferedFileWriter).

} // namespace soratransport::detail2