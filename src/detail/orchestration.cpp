#include "../core.hpp"
#include "internal.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <stdexcept>

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 1024 * 1024;
constexpr std::size_t kMetaQueueDepth = 1024;
constexpr std::size_t kOpenedQueueDepth = 64;
constexpr int kDefaultCompressionLevel = 3;

void join_and_capture(std::jthread& thread, PipelineState& state) {
	try {
		if (thread.joinable()) {
			thread.join();
		}
	} catch (...) {
		state.fail(std::current_exception());
	}
}

boost::asio::awaitable<boost::asio::ip::tcp::socket> connect_socket_async(std::string host, std::uint16_t port) {
	auto executor = co_await boost::asio::this_coro::executor;
	boost::asio::ip::tcp::resolver resolver(executor);
	auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), boost::asio::use_awaitable);
	boost::asio::ip::tcp::socket socket(executor);
	co_await boost::asio::async_connect(socket, endpoints, boost::asio::use_awaitable);
	co_return std::move(socket);
}

boost::asio::awaitable<boost::asio::ip::tcp::socket> accept_socket_async(std::uint16_t port) {
	auto executor = co_await boost::asio::this_coro::executor;
	boost::asio::ip::tcp::acceptor acceptor(executor, {boost::asio::ip::tcp::v4(), port});
	co_return co_await acceptor.async_accept(boost::asio::use_awaitable);
}

boost::asio::awaitable<void> send_directory_task(
	const std::filesystem::path source_dir,
	std::string host,
	std::uint16_t port,
	std::exception_ptr* task_error) {
	try {
		auto socket = co_await connect_socket_async(std::move(host), port);
		SocketByteSink sink(std::move(socket));
		auto config = make_runtime_config();

		RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
		BufferPool pool;
		BoundedQueue<FileMeta> meta_queue(kMetaQueueDepth);
		BoundedQueue<OpenedFileReader> opened_queue(kOpenedQueueDepth);
		ConcurrentDataChunkChannel tar_queue(config.tar_queue_depth);
		DirScanner scanner(executors);
		FileReaderOpener opener(pool, executors, config.reader_threads, kPipelineChunkSize);
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
				ZstdCompressor compressor(pool, executors, kDefaultCompressionLevel);
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

boost::asio::awaitable<void> receive_directory_task(
	std::uint16_t port,
	const std::filesystem::path destination_dir,
	std::exception_ptr* task_error) {
	try {
		auto socket = co_await accept_socket_async(port);
		SocketByteSource source(std::move(socket));
		auto config = make_runtime_config();

		RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
		BufferPool pool;
		ConcurrentDataChunkChannel tar_queue(config.tar_queue_depth);
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

void pack_directory_to_file(const std::filesystem::path& source_dir, const std::filesystem::path& output_file, CompressionMode mode) {
	auto config = make_runtime_config();
	RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
	BufferPool pool;
	BoundedQueue<FileMeta> meta_queue(kMetaQueueDepth);
	BoundedQueue<OpenedFileReader> opened_queue(kOpenedQueueDepth);
	ConcurrentDataChunkChannel tar_queue(config.tar_queue_depth);
	DirScanner scanner(executors);
	FileReaderOpener opener(pool, executors, config.reader_threads, kPipelineChunkSize);
	TarPacker packer(pool, executors, kPipelineChunkSize, config.read_concurrency);
	PipelineState state;

	FileByteSink sink(output_file);
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
			packer.pack(opened_queue, tar_queue);
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	std::jthread sink_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				ZstdCompressor compressor(pool, executors, kDefaultCompressionLevel);
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
	state.rethrow_if_failed();
}

void unpack_file_to_directory(const std::filesystem::path& input_file, const std::filesystem::path& destination_dir, CompressionMode mode) {
	auto config = make_runtime_config();
	RuntimeExecutors executors(config.scanner_threads, config.reader_threads, config.compression_threads);
	BufferPool pool;
	ConcurrentDataChunkChannel tar_queue(config.tar_queue_depth);
	PipelineState state;
	TarUnpacker unpacker(destination_dir);
	FileByteSource source(input_file);

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
			unpacker.unpack(tar_queue);
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	join_and_capture(input_thread, state);
	join_and_capture(unpacker_thread, state);
	state.rethrow_if_failed();
}

void send_directory(const std::filesystem::path& source_dir, std::string_view host, std::uint16_t port) {
	boost::asio::io_context io_context;
	std::exception_ptr task_error;
	boost::asio::co_spawn(io_context, send_directory_task(source_dir, std::string(host), port, &task_error), boost::asio::detached);
	io_context.run();
	if (task_error) {
		std::rethrow_exception(task_error);
	}
}

void receive_directory(std::uint16_t port, const std::filesystem::path& destination_dir) {
	boost::asio::io_context io_context;
	std::exception_ptr task_error;
	boost::asio::co_spawn(io_context, receive_directory_task(port, destination_dir, &task_error), boost::asio::detached);
	io_context.run();
	if (task_error) {
		std::rethrow_exception(task_error);
	}
}

} // namespace soratransport