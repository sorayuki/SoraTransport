#pragma once

#include "io.hpp"

#include <archive.h>
#include <archive_entry.h>

#include <atomic>

namespace soratransport {

struct RuntimeOptions {
	std::optional<std::size_t> max_in_flight_read_bytes;
	std::optional<std::size_t> max_in_flight_write_ops;
	std::optional<int> compression_level;
};

struct QueueTelemetrySample {
	std::size_t size = 0;
	std::size_t capacity = 0;

	double fill_ratio() const {
		if (capacity == 0) {
			return 0.0;
		}
		return static_cast<double>(size) / static_cast<double>(capacity);
	}
};

class DirScanner {
public:
	explicit DirScanner(RuntimeExecutors& executors);
	void scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue) const;

private:
	RuntimeExecutors& executors_;
};

class InFlightReadBudget {
public:
	explicit InFlightReadBudget(std::size_t max_bytes);
	void acquire(std::size_t bytes);
	bool try_acquire(std::size_t bytes);
	void release(std::size_t bytes);
	std::size_t max_bytes() const;
	std::size_t used_bytes() const;

private:
	std::size_t max_bytes_;
	std::size_t used_bytes_ = 0;
	mutable std::mutex mutex_;
	std::condition_variable cv_;
};

class FileReader {
public:
	FileReader(
		BufferPool& pool,
		std::shared_ptr<InFlightReadBudget> read_budget,
		const std::filesystem::path& path,
		std::uint64_t size,
		std::size_t buffer_size,
		FileIoMode io_mode = FileIoMode::Buffered);
	~FileReader();
	FileReader(const FileReader&) = delete;
	FileReader& operator=(const FileReader&) = delete;
	FileReader(FileReader&& other);
	FileReader& operator=(FileReader&& other);
	void open();
	void reserve_prefetch_budget(std::size_t bytes);
	void start_prefetch();
	DataChunk read_next_chunk();
	std::uint64_t offset() const;
	bool eof() const;
	bool is_open() const;

private:
	struct State;
	bool issue_next_read(bool wait_for_budget);
	void prime_prefetch_window();
	void release_slot_budget(std::size_t slot_index);
	void close();
	std::string path_for_error() const;

	BufferPool& pool_;
	std::shared_ptr<InFlightReadBudget> read_budget_;
	std::unique_ptr<State> state_;
};

struct OpenedFileReader {
	FileMeta meta;
	std::optional<FileReader> reader;
};

struct CompressionQueueTelemetry {
	const BoundedQueue<FileMeta>* meta_queue = nullptr;
	const BoundedQueue<OpenedFileReader>* opened_queue = nullptr;
	const BoundedQueue<DataChunk>* tar_queue = nullptr;

	QueueTelemetrySample meta() const {
		return meta_queue == nullptr ? QueueTelemetrySample{} : QueueTelemetrySample{meta_queue->size(), meta_queue->capacity()};
	}

	QueueTelemetrySample opened() const {
		return opened_queue == nullptr ? QueueTelemetrySample{} : QueueTelemetrySample{opened_queue->size(), opened_queue->capacity()};
	}

	QueueTelemetrySample tar() const {
		return tar_queue == nullptr ? QueueTelemetrySample{} : QueueTelemetrySample{tar_queue->size(), tar_queue->capacity()};
	}
};

class FileReaderOpener {
public:
	FileReaderOpener(
		BufferPool& pool,
		RuntimeExecutors& executors,
		std::size_t submit_concurrency,
		std::shared_ptr<InFlightReadBudget> read_budget,
		std::size_t buffer_size,
		FileIoMode io_mode = FileIoMode::Buffered);
	void open(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const;

private:
	BufferPool& pool_;
	RuntimeExecutors& executors_;
	std::size_t submit_concurrency_;
	std::shared_ptr<InFlightReadBudget> read_budget_;
	std::size_t buffer_size_;
	FileIoMode io_mode_;
};

class TarPacker {
public:
	TarPacker(BufferPool& pool, RuntimeExecutors& executors, std::size_t chunk_size, std::size_t read_concurrency);
	void pack(BoundedQueue<OpenedFileReader>& in_meta, BoundedQueue<DataChunk>& out_tar, std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr);

private:
	static la_ssize_t archive_write_callback(struct archive*, void* client_data, const void* buffer, size_t length);
	static int archive_close_callback(struct archive*, void* client_data);
	void add_entry(struct archive* writer, OpenedFileReader& opened_file) const;

	BufferPool& pool_;
	RuntimeExecutors& executors_;
	std::size_t chunk_size_;
	std::size_t read_concurrency_;
};

class TarUnpacker {
public:
	explicit TarUnpacker(const std::filesystem::path& destination_root);
	void unpack(BoundedQueue<DataChunk>& in_tar, std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr);

private:
	static la_ssize_t archive_read_callback(struct archive*, void* client_data, const void** buffer);
	static int archive_close_callback(struct archive*, void* client_data);
	std::filesystem::path resolve_output_path(const char* raw_path) const;

	std::filesystem::path destination_root_;
};

class ZstdCompressor {
public:
	ZstdCompressor(
		BufferPool& pool,
		RuntimeExecutors& executors,
		int compression_level,
		const CompressionQueueTelemetry* queue_telemetry = nullptr);
	void compress(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);

private:
	void compress_sync(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);

	BufferPool& pool_;
	RuntimeExecutors& executors_;
	int compression_level_;
	const CompressionQueueTelemetry* queue_telemetry_;
};

class ZstdDecompressor {
public:
	explicit ZstdDecompressor(BufferPool& pool);
	void decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar);

private:
	BufferPool& pool_;
};

class RawTarWriter {
public:
	void write(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);
};

class RawTarReader {
public:
	explicit RawTarReader(BufferPool& pool);
	void read(IByteSource& source, BoundedQueue<DataChunk>& out_tar);

private:
	BufferPool& pool_;
};

} // namespace soratransport
