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

class TarUnpacker {
public:
	TarUnpacker(
		const std::filesystem::path& destination_root,
		BufferPool& pool,
		RuntimeExecutors& executors,
		std::shared_ptr<InFlightWriteBudget> write_budget,
		std::size_t max_in_flight_write_ops,
		std::size_t max_parallel_extract_files);

	void unpack(
		BoundedQueue<DataChunk>& in_tar,
		std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr,
		std::atomic<std::uint64_t>* file_counter = nullptr,
		const CancelEvent* cancel_event = nullptr);

private:
	std::filesystem::path destination_root_;
	BufferPool* pool_ = nullptr;
	RuntimeExecutors* executors_ = nullptr;
	std::shared_ptr<InFlightWriteBudget> write_budget_;
	std::size_t max_in_flight_write_ops_ = 1;
	std::size_t max_parallel_extract_files_ = 1;
};

} // namespace soratransport::detail2