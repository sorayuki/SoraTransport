#include "detail/internal.hpp"
#include "detail/win32_util.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using soratransport::path_to_utf8_string;

namespace {

constexpr std::size_t kMetaQueueDepth = 1024;
constexpr std::size_t kOpenedQueueDepth = 64;
constexpr std::size_t kPrefetchQueueDepth = 64;
constexpr std::size_t kReadChunkSize = 8 * 1024 * 1024;

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
	soratransport::FileReader& reader,
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

		auto config = soratransport::make_runtime_config();
		soratransport::RuntimeExecutors executors(config.worker_threads);
		soratransport::BufferPool pool;
		auto read_budget = std::make_shared<soratransport::InFlightReadBudget>(config.max_in_flight_read_bytes);
		soratransport::DirScanner scanner(executors);
		soratransport::BoundedQueue<soratransport::FileMeta> meta_queue(kMetaQueueDepth, executors.executor());
		soratransport::BoundedQueue<soratransport::OpenedFileReader> opened_queue(
			std::max(kOpenedQueueDepth, config.file_open_concurrency),
			executors.executor());
		soratransport::BoundedQueue<soratransport::OpenedFileReader> prefetched_queue(kPrefetchQueueDepth, executors.executor());
		soratransport::FileReaderOpener opener(pool, executors, config.file_open_concurrency, kReadChunkSize);
		soratransport::FileReaderPrefetcher prefetcher(executors, read_budget, kReadChunkSize);

		std::uint64_t file_count = 0;
		std::uint64_t bytes_read = 0;
		auto start = std::chrono::steady_clock::now();
		std::exception_ptr opener_error;
		std::mutex opener_error_mutex;

		auto scanner_future = boost::asio::co_spawn(executors.executor(), scanner.scan(root, meta_queue), boost::asio::use_future);
		std::vector<std::jthread> opener_threads;
		std::optional<std::future<void>> prefetch_future;
		if (options.read_files) {
			opener_threads.reserve(config.file_open_concurrency);
			for (std::size_t index = 0; index < config.file_open_concurrency; ++index) {
				opener_threads.emplace_back([&] {
					try {
						opener.open_sync(meta_queue, opened_queue);
					} catch (...) {
						{
							std::lock_guard lock(opener_error_mutex);
							if (!opener_error) {
								opener_error = std::current_exception();
							}
						}
						meta_queue.close();
						opened_queue.close();
						prefetched_queue.close();
					}
				});
			}
			prefetch_future.emplace(boost::asio::co_spawn(executors.executor(), prefetcher.prefetch(opened_queue, prefetched_queue), boost::asio::use_future));
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
			while (auto meta = meta_queue.pop()) {
				if (!std::filesystem::is_regular_file(meta->status)) {
					continue;
				}

				++file_count;
			}
		}

		scanner_future.get();
		for (auto& opener_thread : opener_threads) {
			if (opener_thread.joinable()) {
				opener_thread.join();
			}
		}
		if (opener_error) {
			std::rethrow_exception(opener_error);
		}
		if (options.read_files) {
			opened_queue.close();
		}
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
