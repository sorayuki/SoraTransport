#pragma once

#include "io.hpp"

#include <archive.h>
#include <archive_entry.h>

namespace soratransport {

class DirScanner {
public:
	explicit DirScanner(RuntimeExecutors& executors);
	void scan(const std::filesystem::path& root_dir, BoundedQueue<FileMeta>& out_queue) const;

private:
	RuntimeExecutors& executors_;
};

class FileReader {
public:
	FileReader(BufferPool& pool, const std::filesystem::path& path);
	~FileReader();
	FileReader(const FileReader&) = delete;
	FileReader& operator=(const FileReader&) = delete;
	FileReader(FileReader&& other) noexcept;
	FileReader& operator=(FileReader&& other) noexcept;
	DataChunk read_next_chunk(std::size_t length);
	std::uint64_t offset() const;
	bool eof() const;

private:
	struct State;
	void close();
	void unmap_current_view();
	std::string path_for_error() const;

	BufferPool& pool_;
	std::unique_ptr<State> state_;
};

class TarPacker {
public:
	TarPacker(BufferPool& pool, RuntimeExecutors& executors, std::size_t chunk_size, std::size_t read_concurrency);
	void pack(BoundedQueue<FileMeta>& in_meta, BoundedQueue<DataChunk>& out_tar);
	void pack(BoundedQueue<FileMeta>& in_meta, ConcurrentDataChunkChannel& out_tar);

private:
	static la_ssize_t archive_write_callback(struct archive*, void* client_data, const void* buffer, size_t length);
	static int archive_close_callback(struct archive*, void* client_data);
	void add_entry(struct archive* writer, const FileMeta& meta) const;

	BufferPool& pool_;
	RuntimeExecutors& executors_;
	std::size_t chunk_size_;
	std::size_t read_concurrency_;
};

class TarUnpacker {
public:
	explicit TarUnpacker(const std::filesystem::path& destination_root);
	void unpack(BoundedQueue<DataChunk>& in_tar);
	void unpack(ConcurrentDataChunkChannel& in_tar);

private:
	static la_ssize_t archive_read_callback(struct archive*, void* client_data, const void** buffer);
	static int archive_close_callback(struct archive*, void* client_data);
	std::filesystem::path resolve_output_path(const char* raw_path) const;

	std::filesystem::path destination_root_;
};

class ZstdCompressor {
public:
	ZstdCompressor(BufferPool& pool, RuntimeExecutors& executors, int compression_level);
	void compress(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);
	void compress(ConcurrentDataChunkChannel& in_tar, IByteSink& sink);

private:
	void compress_sync(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);
	void compress_sync(ConcurrentDataChunkChannel& in_tar, IByteSink& sink);

	BufferPool& pool_;
	RuntimeExecutors& executors_;
	int compression_level_;
};

class ZstdDecompressor {
public:
	explicit ZstdDecompressor(BufferPool& pool);
	void decompress(IByteSource& source, BoundedQueue<DataChunk>& out_tar);
	void decompress(IByteSource& source, ConcurrentDataChunkChannel& out_tar);

private:
	BufferPool& pool_;
};

class RawTarWriter {
public:
	void write(BoundedQueue<DataChunk>& in_tar, IByteSink& sink);
	void write(ConcurrentDataChunkChannel& in_tar, IByteSink& sink);
};

class RawTarReader {
public:
	explicit RawTarReader(BufferPool& pool);
	void read(IByteSource& source, BoundedQueue<DataChunk>& out_tar);
	void read(IByteSource& source, ConcurrentDataChunkChannel& out_tar);

private:
	BufferPool& pool_;
};

} // namespace soratransport