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
	bool log_adaptive_compression = false;
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
	boost::asio::awaitable<void> scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue, const CancelEvent* cancel_event = nullptr) const;

private:
	RuntimeExecutors& executors_;
};

class InFlightReadBudget {
public:
	explicit InFlightReadBudget(std::size_t max_bytes);
	void listenCancelSignal(CancelEvent& event);
	void acquire(std::size_t bytes);
	bool try_acquire(std::size_t bytes);
	void release(std::size_t bytes);
	std::size_t max_bytes() const;
	std::size_t used_bytes() const;
	bool is_cancelled() const;

private:
	std::size_t max_bytes_;
	std::size_t used_bytes_ = 0;
	mutable std::mutex mutex_;
	std::condition_variable cv_;
	std::atomic<bool> cancelled_{false};
	boost::signals2::scoped_connection cancel_connection_;
};

class ReadBudgetLease {
public:
	ReadBudgetLease() = default;
	ReadBudgetLease(std::shared_ptr<InFlightReadBudget> budget, std::size_t bytes)
		: budget_(std::move(budget)), bytes_(bytes) {}
	~ReadBudgetLease() {
		reset();
	}

	ReadBudgetLease(const ReadBudgetLease&) = delete;
	ReadBudgetLease& operator=(const ReadBudgetLease&) = delete;

	ReadBudgetLease(ReadBudgetLease&& other) noexcept
		: budget_(std::move(other.budget_)), bytes_(other.bytes_) {
		other.bytes_ = 0;
	}

	ReadBudgetLease& operator=(ReadBudgetLease&& other) noexcept {
		if (this != &other) {
			reset();
			budget_ = std::move(other.budget_);
			bytes_ = other.bytes_;
			other.bytes_ = 0;
		}
		return *this;
	}

	void reset() {
		if (budget_ && bytes_ > 0) {
			budget_->release(bytes_);
		}
		budget_.reset();
		bytes_ = 0;
	}

	std::size_t bytes() const {
		return bytes_;
	}

private:
	std::shared_ptr<InFlightReadBudget> budget_;
	std::size_t bytes_ = 0;
};

class FileReader {
public:
	FileReader(
		BufferPool& pool,
		const std::filesystem::path& path,
		std::uint64_t size,
		std::size_t buffer_size,
		FileIoMode io_mode = FileIoMode::Buffered);
	~FileReader();
	FileReader(const FileReader&) = delete;
	FileReader& operator=(const FileReader&) = delete;
	FileReader(FileReader&& other);
	FileReader& operator=(FileReader&& other);
	void listenCancelSignal(CancelEvent& event);
	void open();
	void start_prefetch(std::size_t max_bytes);
	DataChunk read_next_chunk();
	std::uint64_t offset() const;
	bool eof() const;
	bool is_open() const;
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	bool issue_next_read();
	void prime_prefetch_window(std::size_t max_bytes);
	void close();
	std::string path_for_error() const;

	BufferPool& pool_;
	std::unique_ptr<State> state_;
};

struct OpenedFileReader : IQueueDisposable {
	void Dispose() noexcept override {
		read_budget_lease.reset();
		reader.reset();
		meta = {};
	}

	FileMeta meta;
	std::optional<FileReader> reader;
	ReadBudgetLease read_budget_lease;
};

struct CompressionQueueTelemetry {
	const BoundedQueue<DataChunk>* upstream_queue = nullptr;
	const BoundedQueue<DataChunk>* downstream_queue = nullptr;

	QueueTelemetrySample upstream() const {
		return upstream_queue == nullptr ? QueueTelemetrySample{} : QueueTelemetrySample{upstream_queue->size(), upstream_queue->capacity()};
	}

	QueueTelemetrySample downstream() const {
		return downstream_queue == nullptr ? QueueTelemetrySample{} : QueueTelemetrySample{downstream_queue->size(), downstream_queue->capacity()};
	}
};

class FileReaderOpener {
public:
	FileReaderOpener(
		BufferPool& pool,
		RuntimeExecutors& executors,
		std::size_t submit_concurrency,
		std::size_t buffer_size,
		FileIoMode io_mode = FileIoMode::Buffered,
		CancelEvent* cancel_event = nullptr);
	boost::asio::awaitable<void> open(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const;
	void open_sync(BoundedQueue<FileMeta>& in_meta, BoundedQueue<OpenedFileReader>& out_opened) const;

private:
	BufferPool& pool_;
	RuntimeExecutors& executors_;
	std::size_t submit_concurrency_;
	std::size_t buffer_size_;
	FileIoMode io_mode_;
	CancelEvent* cancel_event_ = nullptr;
};

class FileReaderPrefetcher {
public:
	FileReaderPrefetcher(
		RuntimeExecutors& executors,
		std::shared_ptr<InFlightReadBudget> read_budget,
		std::size_t prefetch_bytes,
		FileIoMode io_mode = FileIoMode::Buffered,
		CancelEvent* cancel_event = nullptr);
	boost::asio::awaitable<void> prefetch(BoundedQueue<OpenedFileReader>& in_opened, BoundedQueue<OpenedFileReader>& out_prefetched) const;

private:
	RuntimeExecutors& executors_;
	std::shared_ptr<InFlightReadBudget> read_budget_;
	std::size_t prefetch_bytes_;
	FileIoMode io_mode_;
	CancelEvent* cancel_event_ = nullptr;
};

class TarPacker {
public:
	TarPacker(BufferPool& pool, std::size_t chunk_size);
	void pack(
		BoundedQueue<OpenedFileReader>& in_meta,
		BoundedQueue<DataChunk>& out_tar,
		std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr,
		std::atomic<std::uint64_t>* file_counter = nullptr,
		const CancelEvent* cancel_event = nullptr);

private:
	static la_ssize_t archive_write_callback(struct archive*, void* client_data, const void* buffer, size_t length);
	static int archive_close_callback(struct archive*, void* client_data);
	void add_entry(struct archive* writer, OpenedFileReader& opened_file, std::atomic<std::uint64_t>* file_counter, const CancelEvent* cancel_event) const;

	BufferPool& pool_;
	std::size_t chunk_size_;
};

class TarUnpacker {
public:
	explicit TarUnpacker(const std::filesystem::path& destination_root);
	void unpack(
		BoundedQueue<DataChunk>& in_tar,
		std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr,
		std::atomic<std::uint64_t>* file_counter = nullptr,
		const CancelEvent* cancel_event = nullptr);

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
		const CompressionQueueTelemetry* queue_telemetry = nullptr,
		std::atomic<int>* active_level = nullptr,
		bool log_adaptive_decisions = false);
	void compress(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd, const CancelEvent* cancel_event = nullptr);

private:
	void compress_sync(BoundedQueue<DataChunk>& in_tar, BoundedQueue<DataChunk>& out_zstd, const CancelEvent* cancel_event);

	BufferPool& pool_;
	RuntimeExecutors& executors_;
	int compression_level_;
	const CompressionQueueTelemetry* queue_telemetry_;
	std::atomic<int>* active_level_;
	bool log_adaptive_decisions_ = false;
};

class ZstdDecompressor {
public:
	explicit ZstdDecompressor(BufferPool& pool);
	void decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event = nullptr);

private:
	BufferPool& pool_;
};

class RawTarWriter {
public:
	void write(BoundedQueue<DataChunk>& in_tar, IByteSink& sink, const CancelEvent* cancel_event = nullptr);
};

class QueueWriter {
public:
	void write(BoundedQueue<DataChunk>& in_queue, IByteSink& sink, const CancelEvent* cancel_event = nullptr);
};

class RawTarReader {
public:
	explicit RawTarReader(BufferPool& pool);
	void read(IByteSource& source, BoundedQueue<DataChunk>& out_tar, const CancelEvent* cancel_event = nullptr);

private:
	BufferPool& pool_;
};

} // namespace soratransport
