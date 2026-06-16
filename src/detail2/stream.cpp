#include "stream.hpp"

namespace soratransport::detail2 {

void QueueWriter::write(BoundedQueue<DataChunk>& in_queue, IByteSink& sink, const CancelEvent* cancel_event) {
	::soratransport::QueueWriter writer;
	writer.write(in_queue, sink, cancel_event);
}

RawTarReader::RawTarReader(BufferPool& pool) : pool_(&pool) {}

void RawTarReader::read(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event) {
	::soratransport::RawTarReader reader(*pool_);
	reader.read(source, out_tar, cancel_event);
}

ZstdDecompressor::ZstdDecompressor(BufferPool& pool) : pool_(&pool) {}

void ZstdDecompressor::decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event) {
	::soratransport::ZstdDecompressor decompressor(*pool_);
	decompressor.decompress(source, out_tar, cancel_event);
}

} // namespace soratransport::detail2