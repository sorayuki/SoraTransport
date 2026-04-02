#include "detail/internal.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace {

constexpr std::size_t kMetaQueueDepth = 1024;
constexpr std::size_t kOpenedQueueDepth = 64;
constexpr std::size_t kReadChunkSize = 1024 * 1024;

std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	std::string result;
	result.reserve(utf8.size());
	for (char8_t ch : utf8) {
		result.push_back(static_cast<char>(ch));
	}
	return result;
}

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
		auto chunk = reader.read_next_chunk(kReadChunkSize);
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
		const Options options = parse_options(argc, argv);
		const std::filesystem::path& root = options.root;

		std::error_code ec;
		if (!std::filesystem::exists(root, ec) || ec) {
			std::cerr << "Path does not exist: " << path_to_utf8_string(root) << '\n';
			return 1;
		}

		if (!std::filesystem::is_directory(root, ec) || ec) {
			std::cerr << "Path is not a directory: " << path_to_utf8_string(root) << '\n';
			return 1;
		}

		const auto config = soratransport::make_runtime_config();
		soratransport::RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
		soratransport::BufferPool pool;
		soratransport::DirScanner scanner(executors);
		soratransport::BoundedQueue<soratransport::FileMeta> meta_queue(kMetaQueueDepth);
		soratransport::BoundedQueue<soratransport::OpenedFileReader> opened_queue(kOpenedQueueDepth);
		soratransport::FileReaderOpener opener(pool, executors, config.reader_threads);

		std::exception_ptr scanner_error;
		std::exception_ptr opener_error;
		std::uint64_t file_count = 0;
		std::uint64_t bytes_read = 0;
		auto start = std::chrono::steady_clock::now();

		std::jthread scanner_thread([&] {
			try {
				scanner.scan(root, meta_queue);
			} catch (...) {
				scanner_error = std::current_exception();
				meta_queue.close();
			}
		});

		std::optional<std::jthread> opener_thread;
		if (options.read_files) {
			opener_thread.emplace([&] {
				try {
					opener.open(meta_queue, opened_queue);
				} catch (...) {
					opener_error = std::current_exception();
					meta_queue.close();
					opened_queue.close();
				}
			});
		}

		if (options.read_files) {
			while (auto opened = opened_queue.pop()) {
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

		if (scanner_thread.joinable()) {
			scanner_thread.join();
		}
		if (opener_thread.has_value() && opener_thread->joinable()) {
			opener_thread->join();
		}

		const auto end = std::chrono::steady_clock::now();
		const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

		if (scanner_error) {
			std::rethrow_exception(scanner_error);
		}
		if (opener_error) {
			std::rethrow_exception(opener_error);
		}
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