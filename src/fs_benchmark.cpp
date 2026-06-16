#include "detail2/config.hpp"
#include "detail2/filesystem.hpp"
#include "detail/win32_util.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>

using soratransport::path_to_utf8_string;

namespace {

constexpr std::size_t kOpenedQueueDepth = 64;
constexpr std::size_t kPrefetchQueueDepth = 64;

struct Options {
	std::filesystem::path root = "D:/dev/boost_1_90_0/dist";
	bool read_files = false;
};

Options parse_options(int argc, char** argv) {
	Options options;
	for (int index = 1; index < argc; ++index) {
		const std::string arg = argv[index];
		if (arg == "--read-files") {
			options.read_files = true;
			continue;
		}
		if (arg == "--help" || arg == "-h") {
			std::cout << "Usage: fs_benchmark [--read-files] [root-path]\n";
			std::cout << "  --read-files  Read and discard file contents after traversal reaches each file\n";
			std::cout << "  root-path     Directory to traverse, default is D:/dev/boost_1_90_0/dist\n";
			exit(0);
		}
		options.root = arg;
	}
	return options;
}

std::uint64_t read_file_and_discard(
	soratransport::detail2::SequentialFileReader& reader,
	const soratransport::FileMeta& meta) {
	std::uint64_t total_bytes = 0;
	while (!reader.eof()) {
		auto chunk = reader.read_next_chunk();
		if (chunk.length == 0 && !reader.eof()) {
			throw std::runtime_error("Failed to read file data");
		}
		total_bytes += chunk.length;
	}
	if (total_bytes != meta.size) {
		throw std::runtime_error("File size mismatch after sequential read");
	}
	return total_bytes;
}

} // namespace

int main(int argc, char** argv) {
	try {
		auto options = parse_options(argc, argv);
		auto& root = options.root;

		std::error_code ec;
		if (!std::filesystem::exists(root, ec) || ec) {
			std::cerr << "Path does not exist: " << path_to_utf8_string(root) << '\n';
			return 1;
		}
		if (!std::filesystem::is_directory(root, ec) || ec) {
			std::cerr << "Path is not a directory: " << path_to_utf8_string(root) << '\n';
			return 1;
		}

		namespace d2 = soratransport::detail2;
		auto tuning = d2::make_pipeline_tuning();
		d2::TaskExecutor executor(tuning.worker_threads);
		soratransport::BufferPool pool;
		d2::FileTraverser traverser(executor.executor(), tuning);
		d2::FileOpener opener(pool, executor, tuning);
		d2::FilePrefetcher prefetcher(executor.executor(), tuning);

		using soratransport::BoundedQueue;
		BoundedQueue<d2::TraversalEntry> traversal_queue(tuning.opened_queue_capacity, executor.executor());
		BoundedQueue<d2::OpenedFile> opened_queue(
			std::max(kOpenedQueueDepth, tuning.file_open_concurrency),
			executor.executor());
		BoundedQueue<d2::OpenedFile> prefetched_queue(kPrefetchQueueDepth, executor.executor());

		std::uint64_t file_count = 0;
		std::uint64_t bytes_read = 0;
		auto start = std::chrono::steady_clock::now();

		auto traverse_future = boost::asio::co_spawn(
			executor.executor(), traverser.traverse(root, traversal_queue), boost::asio::use_future);
		auto open_future = boost::asio::co_spawn(
			executor.executor(), opener.open(traversal_queue, opened_queue), boost::asio::use_future);

		std::optional<std::future<void>> prefetch_future;
		if (options.read_files) {
			prefetch_future.emplace(boost::asio::co_spawn(
				executor.executor(), prefetcher.prefetch(opened_queue, prefetched_queue), boost::asio::use_future));
		}

		if (options.read_files) {
			while (auto opened = prefetched_queue.pop()) {
				if (!std::filesystem::is_regular_file(opened->meta.status)) {
					continue;
				}
				++file_count;
				if (!opened->reader.has_value()) {
					throw std::runtime_error("Opened reader is missing for regular file");
				}
				bytes_read += read_file_and_discard(*opened->reader, opened->meta);
			}
		} else {
			while (auto opened = opened_queue.pop()) {
				if (!std::filesystem::is_regular_file(opened->meta.status)) {
					continue;
				}
				++file_count;
			}
		}

		traverse_future.get();
		open_future.get();
		if (prefetch_future.has_value()) {
			prefetch_future->get();
		}

		auto end = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

		std::cout << "Root: " << path_to_utf8_string(root) << '\n';
		std::cout << "Read files: " << (options.read_files ? "enabled" : "disabled") << '\n';
		std::cout << "Files visited: " << file_count << '\n';
		if (options.read_files) {
			std::cout << "Bytes read: " << bytes_read << '\n';
		}
		std::cout << "Elapsed: " << elapsed.count() << " ms" << '\n';
		return 0;
	} catch (const std::exception& ex) {
		std::cerr << ex.what() << '\n';
		return 1;
	}
}
