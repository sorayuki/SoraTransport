#pragma once

#include "chunk.hpp"
#include "config.hpp"
#include "filesystem.hpp"
#include "writer.hpp"

#include <archive.h>
#include <archive_entry.h>

#include <atomic>
#include <filesystem>

namespace soratransport::detail2 {

class TarPacker {
public:
	TarPacker(BufferPool& pool, std::size_t chunk_size, SemaphoreCor* output_budget = nullptr);
	void pack(
		BoundedQueue<OpenedFile>& in_files,
		BoundedQueue<DataChunk>& out_tar,
		std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr,
		std::atomic<std::uint64_t>* file_counter = nullptr,
		const CancelEvent* cancel_event = nullptr);

private:
	static la_ssize_t archive_write_callback(struct archive*, void* client_data, const void* buffer, size_t length);
	static int archive_close_callback(struct archive*, void* client_data);
	void add_entry(struct archive* writer, OpenedFile& opened_file, std::atomic<std::uint64_t>* file_counter, const CancelEvent* cancel_event) const;

	BufferPool& pool_;
	std::size_t chunk_size_;
	SemaphoreCor* output_budget_ = nullptr;
};

class TarUnpacker {
public:
	TarUnpacker(
		const std::filesystem::path& destination_root,
		BufferPool& pool,
		TaskExecutor& executor,
		PipelineTuning tuning,
		CancelEvent* cancel_event = nullptr);

	void unpack(
		BoundedQueue<DataChunk>& in_tar,
		std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr,
		std::atomic<std::uint64_t>* file_counter = nullptr,
		const CancelEvent* cancel_event = nullptr);

private:
	static la_ssize_t archive_read_callback(struct archive*, void* client_data, const void** buffer);
	static int archive_close_callback(struct archive*, void* client_data);
	std::filesystem::path resolve_output_path(const char* utf8_path) const;

	std::filesystem::path destination_root_;
	BufferPool& pool_;
	TaskExecutor& executor_;
	PipelineTuning tuning_;
	CancelEvent* cancel_event_ = nullptr;
};

} // namespace soratransport::detail2