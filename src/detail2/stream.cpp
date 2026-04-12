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

TarUnpacker::TarUnpacker(
	const std::filesystem::path& destination_root,
	BufferPool& pool,
	RuntimeExecutors& executors,
	std::shared_ptr<InFlightWriteBudget> write_budget,
	std::size_t max_in_flight_write_ops,
	std::size_t max_parallel_extract_files)
	: destination_root_(destination_root),
	  pool_(&pool),
	  executors_(&executors),
	  write_budget_(std::move(write_budget)),
	  max_in_flight_write_ops_(max_in_flight_write_ops),
	  max_parallel_extract_files_(max_parallel_extract_files) {}

void TarUnpacker::unpack(
	BoundedQueue<DataChunk>& in_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	::soratransport::TarUnpacker unpacker(
		destination_root_,
		*pool_,
		*executors_,
		write_budget_,
		max_in_flight_write_ops_,
		max_parallel_extract_files_);
	unpacker.unpack(in_tar, uncompressed_bytes_counter, file_counter, cancel_event);
}

} // namespace soratransport::detail2