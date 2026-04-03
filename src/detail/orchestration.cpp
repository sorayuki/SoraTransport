#include "../core.hpp"
#include "internal.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace asio = boost::asio;

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;
constexpr std::size_t kMetaQueueDepth = 256;
constexpr std::size_t kOpenedQueueDepth = 32;
constexpr int kDefaultCompressionLevel = 3;

std::string format_scaled_bytes(std::uint64_t bytes, std::string_view suffix) {
	static constexpr const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
	double value = static_cast<double>(bytes);
	std::size_t unit_index = 0;
	while (value >= 1024.0 && unit_index + 1 < std::size(units)) {
		value /= 1024.0;
		++unit_index;
	}

	std::ostringstream out;
	out << std::fixed << std::setprecision(value >= 100.0 ? 0 : 1) << value << ' ' << units[unit_index] << suffix;
	return out.str();
}

std::string format_rate(std::uint64_t bytes_per_second) {
	return format_scaled_bytes(bytes_per_second, "/s");
}

std::string queue_usage(std::string_view name, std::size_t size, std::size_t capacity) {
	std::ostringstream out;
	out << name << ':' << size << '/' << capacity;
	if (capacity > 0) {
		const auto ratio = static_cast<double>(size) * 100.0 / static_cast<double>(capacity);
		out << '(' << std::fixed << std::setprecision(0) << ratio << "%)";
	}
	return out.str();
}

void print_pack_progress_legend() {
	std::cerr
		<< "progress legend (pack):\n"
		<< "  rate: uncompressed tar stream throughput per second\n"
		<< "  q{s/o o/t t/s}: queue usage current/capacity(percent)\n"
		<< "    s/o = scanner -> opener\n"
		<< "    o/t = opener -> tar packer\n"
		<< "    t/s = tar -> sink(compressor/file writer)\n"
		<< "  rb: in-flight file read budget used/limit\n";
}

void print_unpack_progress_legend() {
	std::cerr
		<< "progress legend (unpack):\n"
		<< "  rate: uncompressed tar stream throughput per second\n"
		<< "  q{s/u}: queue usage current/capacity(percent), s/u = source -> unpacker\n";
}

void clear_progress_line() {
	std::cerr << "\r\x1b[2K";
}

void print_progress_line(const std::string& line) {
	clear_progress_line();
	std::cerr << line << std::flush;
}

void finalize_progress_line() {
	std::cerr << '\n' << std::flush;
}

std::jthread start_progress_thread(std::function<std::string(std::uint64_t)> build_line, std::atomic<std::uint64_t>& processed_bytes) {
	return std::jthread([build_line = std::move(build_line), &processed_bytes](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous = processed_bytes.load(std::memory_order_relaxed);
		while (!stop_token.stop_requested()) {
			std::this_thread::sleep_for(1s);
			if (stop_token.stop_requested()) {
				break;
			}
			const auto current = processed_bytes.load(std::memory_order_relaxed);
			const auto delta = current - previous;
			previous = current;
			print_progress_line(build_line(delta));
		}
	});
}

void join_and_capture(std::jthread& thread, PipelineState& state) {
	try {
		if (thread.joinable()) {
			thread.join();
		}
	} catch (...) {
		state.fail(std::current_exception());
	}
}

asio::awaitable<asio::ip::tcp::socket> connect_socket_async(std::string host, std::uint16_t port) {
	auto executor = co_await asio::this_coro::executor;
	asio::ip::tcp::resolver resolver(executor);
	auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::use_awaitable);
	asio::ip::tcp::socket socket(executor);
	co_await asio::async_connect(socket, endpoints, asio::use_awaitable);
	co_return std::move(socket);
}

asio::awaitable<asio::ip::tcp::socket> accept_socket_async(std::uint16_t port) {
	auto executor = co_await asio::this_coro::executor;
	asio::ip::tcp::acceptor acceptor(executor, {asio::ip::tcp::v4(), port});
	co_return co_await acceptor.async_accept(asio::use_awaitable);
}

asio::awaitable<void> send_directory_task(
	const std::filesystem::path source_dir,
	std::string host,
	std::uint16_t port,
	RuntimeOptions options,
	std::exception_ptr* task_error) {
	try {
		auto socket = co_await connect_socket_async(std::move(host), port);
		SocketByteSink sink(std::move(socket));
		auto config = make_runtime_config(options);
		const auto compression_level = options.compression_level.value_or(kDefaultCompressionLevel);
		const auto enable_adaptive_compression = !options.compression_level.has_value();

		RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
		BufferPool pool;
		auto read_budget = std::make_shared<InFlightReadBudget>(config.max_in_flight_read_bytes);
		BoundedQueue<FileMeta> meta_queue(kMetaQueueDepth);
		BoundedQueue<OpenedFileReader> opened_queue(kOpenedQueueDepth);
		BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth);
		CompressionQueueTelemetry compression_telemetry{&meta_queue, &opened_queue, &tar_queue};
		DirScanner scanner(executors);
		FileReaderOpener opener(pool, executors, config.reader_threads, read_budget, kPipelineChunkSize);
		TarPacker packer(pool, executors, kPipelineChunkSize, config.read_concurrency);
		PipelineState state;

		std::jthread scanner_thread([&] {
			try {
				scanner.scan(source_dir, meta_queue);
			} catch (...) {
				state.fail(std::current_exception());
				meta_queue.close();
			}
		});

		std::jthread packer_thread([&] {
			try {
				opener.open(meta_queue, opened_queue);
			} catch (...) {
				state.fail(std::current_exception());
				meta_queue.close();
				opened_queue.close();
			}
		});

		std::jthread sender_thread([&] {
			try {
				packer.pack(opened_queue, tar_queue);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
			}
		});

		std::jthread compressor_thread([&] {
			try {
				ZstdCompressor compressor(
					pool,
					executors,
					compression_level,
					enable_adaptive_compression ? &compression_telemetry : nullptr);
				compressor.compress(tar_queue, sink);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
			}
		});

		join_and_capture(scanner_thread, state);
		join_and_capture(packer_thread, state);
		join_and_capture(sender_thread, state);
		join_and_capture(compressor_thread, state);
		state.rethrow_if_failed();
	} catch (...) {
		*task_error = std::current_exception();
	}

	co_return;
}

asio::awaitable<void> receive_directory_task(
	std::uint16_t port,
	const std::filesystem::path destination_dir,
	std::exception_ptr* task_error) {
	try {
		auto socket = co_await accept_socket_async(port);
		SocketByteSource source(std::move(socket));
		auto config = make_runtime_config();

		RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
		BufferPool pool;
		BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth);
		PipelineState state;
		TarUnpacker unpacker(destination_dir);

		std::jthread input_thread([&] {
			try {
				ZstdDecompressor decompressor(pool);
				decompressor.decompress(source, tar_queue);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
			}
		});

		std::jthread unpacker_thread([&] {
			try {
				unpacker.unpack(tar_queue);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
			}
		});

		join_and_capture(input_thread, state);
		join_and_capture(unpacker_thread, state);
		state.rethrow_if_failed();
	} catch (...) {
		*task_error = std::current_exception();
	}

	co_return;
}

} // namespace

void pack_directory_to_file(const std::filesystem::path& source_dir, const std::filesystem::path& output_file, CompressionMode mode, FileIoMode file_io_mode, RuntimeOptions options) {
	auto config = make_runtime_config(options);
	const auto compression_level = options.compression_level.value_or(kDefaultCompressionLevel);
	const auto enable_adaptive_compression = mode == CompressionMode::Zstd && !options.compression_level.has_value();
	RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
	BufferPool pool;
	auto read_budget = std::make_shared<InFlightReadBudget>(config.max_in_flight_read_bytes);
	BoundedQueue<FileMeta> meta_queue(kMetaQueueDepth);
	BoundedQueue<OpenedFileReader> opened_queue(kOpenedQueueDepth);
	BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth);
	CompressionQueueTelemetry compression_telemetry{&meta_queue, &opened_queue, &tar_queue};
	DirScanner scanner(executors);
	FileReaderOpener opener(pool, executors, config.reader_threads, read_budget, kPipelineChunkSize, file_io_mode);
	TarPacker packer(pool, executors, kPipelineChunkSize, config.read_concurrency);
	PipelineState state;
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	print_pack_progress_legend();

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[pack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/o", meta_queue.size(), meta_queue.capacity())
				<< ' ' << queue_usage("o/t", opened_queue.size(), opened_queue.capacity())
				<< ' ' << queue_usage("t/s", tar_queue.size(), tar_queue.capacity()) << '}'
				<< " rb=" << format_scaled_bytes(read_budget->used_bytes(), "")
				<< '/' << format_scaled_bytes(read_budget->max_bytes(), "");
			return out.str();
		},
		uncompressed_bytes_processed);

	FileByteSink sink(output_file, file_io_mode, config.max_in_flight_write_ops);
	std::jthread scanner_thread([&] {
		try {
			scanner.scan(source_dir, meta_queue);
		} catch (...) {
			state.fail(std::current_exception());
			meta_queue.close();
		}
	});

	std::jthread packer_thread([&] {
		try {
			opener.open(meta_queue, opened_queue);
		} catch (...) {
			state.fail(std::current_exception());
			meta_queue.close();
			opened_queue.close();
		}
	});

	std::jthread writer_thread([&] {
		try {
			packer.pack(opened_queue, tar_queue, &uncompressed_bytes_processed);
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	std::jthread sink_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				ZstdCompressor compressor(
					pool,
					executors,
					compression_level,
					enable_adaptive_compression ? &compression_telemetry : nullptr);
				compressor.compress(tar_queue, sink);
			} else {
				RawTarWriter writer;
				writer.write(tar_queue, sink);
			}
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	join_and_capture(scanner_thread, state);
	join_and_capture(packer_thread, state);
	join_and_capture(writer_thread, state);
	join_and_capture(sink_thread, state);
	progress_thread.request_stop();
	if (progress_thread.joinable()) {
		progress_thread.join();
	}
	finalize_progress_line();
	state.rethrow_if_failed();
}

void unpack_file_to_directory(const std::filesystem::path& input_file, const std::filesystem::path& destination_dir, CompressionMode mode, FileIoMode file_io_mode, RuntimeOptions options) {
	auto config = make_runtime_config(options);
	RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
	BufferPool pool;
	BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth);
	PipelineState state;
	TarUnpacker unpacker(destination_dir);
	FileByteSource source(input_file, file_io_mode);
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	print_unpack_progress_legend();

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[unpack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/u", tar_queue.size(), tar_queue.capacity()) << '}';
			return out.str();
		},
		uncompressed_bytes_processed);

	std::jthread input_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				ZstdDecompressor decompressor(pool);
				decompressor.decompress(source, tar_queue);
			} else {
				RawTarReader reader(pool);
				reader.read(source, tar_queue);
			}
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	std::jthread unpacker_thread([&] {
		try {
			unpacker.unpack(tar_queue, &uncompressed_bytes_processed);
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	join_and_capture(input_thread, state);
	join_and_capture(unpacker_thread, state);
	progress_thread.request_stop();
	if (progress_thread.joinable()) {
		progress_thread.join();
	}
	finalize_progress_line();
	state.rethrow_if_failed();
}

void send_directory(const std::filesystem::path& source_dir, std::string_view host, std::uint16_t port, RuntimeOptions options) {
	asio::io_context io_context;
	std::exception_ptr task_error;
	asio::co_spawn(io_context, send_directory_task(source_dir, std::string(host), port, options, &task_error), asio::detached);
	io_context.run();
	if (task_error) {
		std::rethrow_exception(task_error);
	}
}

void receive_directory(std::uint16_t port, const std::filesystem::path& destination_dir) {
	asio::io_context io_context;
	std::exception_ptr task_error;
	asio::co_spawn(io_context, receive_directory_task(port, destination_dir, &task_error), asio::detached);
	io_context.run();
	if (task_error) {
		std::rethrow_exception(task_error);
	}
}

} // namespace soratransport
