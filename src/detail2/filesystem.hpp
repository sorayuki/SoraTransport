#pragma once

#include "config.hpp"
#include "infra.hpp"

#include "../detail/io.hpp"
#include "../detail/runtime.hpp"

#include <boost/asio/awaitable.hpp>

#include <filesystem>
#include <future>
#include <optional>

namespace soratransport::detail2 {

struct TraversalEntry : IQueueDisposable {
	void Dispose() noexcept override {
		meta = {};
		sequence = 0;
		regular_file = false;
	}

	FileMeta meta;
	std::size_t sequence = 0;
	bool regular_file = false;
};

class SequentialFileReader {
public:
	SequentialFileReader(
		BufferPool& pool,
		const std::filesystem::path& path,
		std::uint64_t size,
		std::size_t buffer_size,
		HANDLE handle);
	~SequentialFileReader();
	SequentialFileReader(const SequentialFileReader&) = delete;
	SequentialFileReader& operator=(const SequentialFileReader&) = delete;
	SequentialFileReader(SequentialFileReader&& other);
	SequentialFileReader& operator=(SequentialFileReader&& other);
	void listenCancelSignal(CancelEvent& event);
	void start_prefetch(std::size_t max_bytes);
	DataChunk read_next_chunk();
	std::uint64_t offset() const;
	bool eof() const;
	bool is_open() const;
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	std::unique_ptr<OverlappedFileReader> reader_;
};

struct OpenedFile : IQueueDisposable {
	void Dispose() noexcept override {
		reader.reset();
		prefetch_guard.reset();
		open_guard.reset();
		prefetched_bytes = 0;
		meta = {};
	}

	FileMeta meta;
	std::optional<SequentialFileReader> reader;
	SemaphoreCor::Guard open_guard;
	SemaphoreCor::Guard prefetch_guard;
	std::size_t prefetched_bytes = 0;
};

class FileTraverser {
public:
	FileTraverser(boost::asio::any_io_executor executor, PipelineTuning tuning, CancelEvent* cancel_event = nullptr);
	boost::asio::awaitable<void> traverse(const std::filesystem::path& root, BoundedQueue<TraversalEntry>& out_queue) const;
	boost::asio::awaitable<void> traverse(const std::vector<std::filesystem::path>& roots, BoundedQueue<TraversalEntry>& out_queue) const;

private:
	PipelineTuning tuning_;
	CancelEvent* cancel_event_ = nullptr;
	mutable SemaphoreCor directory_slots_;
	boost::signals2::scoped_connection cancel_connection_;
};

class FileOpener {
public:
	FileOpener(BufferPool& pool, TaskExecutor& executor, PipelineTuning tuning, CancelEvent* cancel_event = nullptr);
	boost::asio::awaitable<void> open(BoundedQueue<TraversalEntry>& in_entries, BoundedQueue<OpenedFile>& out_opened) const;

private:
	BufferPool& pool_;
	TaskExecutor& executor_;
	PipelineTuning tuning_;
	CancelEvent* cancel_event_ = nullptr;
	mutable SemaphoreCor open_slots_;
	boost::signals2::scoped_connection cancel_connection_;
};

class FilePrefetcher {
public:
	FilePrefetcher(boost::asio::any_io_executor executor, PipelineTuning tuning, CancelEvent* cancel_event = nullptr);
	boost::asio::awaitable<void> prefetch(BoundedQueue<OpenedFile>& in_opened, BoundedQueue<OpenedFile>& out_prefetched) const;
	std::size_t used_budget_bytes() const;
	std::size_t total_budget_bytes() const;

private:
	PipelineTuning tuning_;
	CancelEvent* cancel_event_ = nullptr;
	mutable SemaphoreCor byte_budget_;
	boost::signals2::scoped_connection cancel_connection_;
};

std::size_t compute_prefetch_bytes(const OpenedFile& opened_file, const PipelineTuning& tuning);

void populate_file_metadata(FileMeta& meta);

// ============================================================================
// 文件对比工具（用于控制通道文件信息交换）
// ============================================================================

// 比较 FileInfoEntry 与本地 base_dir/relative_path 处的文件
// 返回 true 表示文件不同（大小或 mtime 不匹配，或本地文件不存在）
bool file_differs_from_local(
	const std::filesystem::path& base_dir,
	const std::string& relative_path,
	std::uint64_t expected_size,
	std::int64_t expected_mtime_sec,
	long expected_mtime_nsec);

} // namespace soratransport::detail2