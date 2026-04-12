#include "../core.hpp"
#include "internal.hpp"
#include "win32_util.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <iostream>
#include <stop_token>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace asio = boost::asio;

namespace soratransport {

namespace {

constexpr std::size_t kPipelineChunkSize = 4 * 1024 * 1024;
constexpr std::size_t kOpenedQueueDepth = 32;
constexpr std::size_t kPrefetchQueueDepth = 64;
constexpr int kDefaultCompressionLevel = 3;

std::runtime_error make_boost_error(const std::string& message, const boost::system::error_code& error) {
	if (error.category() == boost::system::system_category()) {
		return std::runtime_error(message + ": " + win32_error_message_utf8(static_cast<DWORD>(error.value())));
	}
	return std::runtime_error(message + ": " + error.message());
}

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
	return out.str();
}

void print_pack_progress_legend(CompressionMode mode) {
	std::cerr
		<< "progress legend (pack):\n"
		<< "  rate: uncompressed tar stream throughput per second\n";
	if (mode == CompressionMode::Zstd) {
		std::cerr
			<< "  q{s/p p/t t/z z/s}: queue usage current/capacity\n"
			<< "    s/p = scanner -> prefetcher\n"
			<< "    p/t = prefetcher -> tar packer\n"
			<< "    t/z = tar packer -> zstd compressor\n"
			<< "    z/s = zstd compressor -> sink writer\n"
			<< "  lvl: active zstd compression level\n";
	} else {
		std::cerr
			<< "  q{s/p p/t t/s}: queue usage current/capacity\n"
			<< "    s/p = scanner -> prefetcher\n"
			<< "    p/t = prefetcher -> tar packer\n"
			<< "    t/s = tar packer -> sink writer\n";
	}
	std::cerr << "  rb: in-flight file read budget used/limit\n";
}

void print_unpack_progress_legend() {
	std::cerr
		<< "progress legend (unpack):\n"
		<< "  rate: uncompressed tar stream throughput per second\n"
		<< "  q{s/u}: queue usage current/capacity, s/u = source -> unpacker\n"
		<< "  wb: in-flight extracted write budget used/limit\n";
}

void print_listen_progress_legend() {
	std::cerr
		<< "progress legend (listen):\n"
		<< "  rate: uncompressed tar stream throughput per second\n"
		<< "  q{s/p p/t t/z z/n}: queue usage current/capacity\n"
		<< "    s/p = scanner -> prefetcher\n"
		<< "    p/t = prefetcher -> tar packer\n"
		<< "    t/z = tar packer -> zstd compressor\n"
		<< "    z/n = zstd compressor -> network sender\n"
		<< "  lvl: active zstd compression level\n"
		<< "  rb: in-flight file read budget used/limit\n";
}

void print_receive_progress_legend() {
	std::cerr
		<< "progress legend (receive):\n"
		<< "  rate: uncompressed tar stream throughput per second\n"
		<< "  q{n/u}: queue usage current/capacity, n/u = network source -> unpacker\n"
		<< "  wb: in-flight extracted write budget used/limit\n";
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

void set_progress_status(const std::shared_ptr<TransferProgress>& progress, std::string status) {
	if (progress) {
		progress->set_status(std::move(status));
	}
}

void complete_progress(
	const std::shared_ptr<TransferProgress>& progress,
	bool failed,
	bool cancelled,
	std::string status) {
	if (!progress) {
		return;
	}

	if (failed) {
		progress->set_failed(std::move(status));
	} else if (cancelled) {
		progress->set_cancelled(std::move(status));
	} else {
		progress->set_completed(std::move(status));
	}
}

std::jthread start_progress_sync_thread(
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint64_t>& processed_bytes,
	std::atomic<std::uint64_t>& processed_files) {
	return std::jthread([progress, &processed_bytes, &processed_files](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous_bytes = 0;
		std::uint64_t previous_files = 0;
		while (!stop_token.stop_requested()) {
			std::this_thread::sleep_for(250ms);
			const auto current_bytes = processed_bytes.load(std::memory_order_relaxed);
			const auto current_files = processed_files.load(std::memory_order_relaxed);
			if (progress) {
				progress->add_processed_bytes(current_bytes - previous_bytes);
				progress->add_processed_files(current_files - previous_files);
			}
			previous_bytes = current_bytes;
			previous_files = current_files;
		}
		if (progress) {
			const auto current_bytes = processed_bytes.load(std::memory_order_relaxed);
			const auto current_files = processed_files.load(std::memory_order_relaxed);
			progress->add_processed_bytes(current_bytes - previous_bytes);
			progress->add_processed_files(current_files - previous_files);
		}
	});
}

bool is_transfer_cancelled(const std::exception_ptr& error) {
	if (!error) {
		return false;
	}
	try {
		std::rethrow_exception(error);
	} catch (const CancelledError&) {
		return true;
	} catch (...) {
		return false;
	}
}

bool should_throw_cancelled_error(const CancelEvent& cancel_event, const boost::system::error_code& error) {
	return cancel_event.is_cancelled() && (error == asio::error::operation_aborted || error == asio::error::bad_descriptor);
}

bool should_report_transfer_error(const CancelEvent& cancel_event, const std::exception_ptr& error) {
	return !cancel_event.is_cancelled() && !is_transfer_cancelled(error);
}

asio::awaitable<asio::ip::tcp::socket> connect_socket_async(std::string host, std::uint16_t port, CancelEvent& cancel_event) {
	auto executor = co_await asio::this_coro::executor;
	asio::ip::tcp::resolver resolver(executor);
	asio::ip::tcp::socket socket(executor);
	auto on_cancel = cancel_event.connect([&resolver, &socket] {
		resolver.cancel();
		boost::system::error_code ignored;
		socket.cancel(ignored);
		socket.close(ignored);
	});
	boost::system::error_code error;
	auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::redirect_error(asio::use_awaitable, error));
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to resolve receiver address", error);
	}
	co_await asio::async_connect(socket, endpoints, asio::redirect_error(asio::use_awaitable, error));
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to connect to sender", error);
	}
	co_return socket;
}

std::string make_websocket_host_header(std::string_view host, std::uint16_t port) {
	const auto host_text = std::string(host);
	if (host_text.find(':') != std::string::npos && (host_text.empty() || host_text.front() != '[')) {
		return std::string("[") + host_text + "]:" + std::to_string(port);
	}
	return host_text + ':' + std::to_string(port);
}

asio::awaitable<TransportWebSocket> connect_websocket_async(std::string host, std::uint16_t port, CancelEvent& cancel_event) {
	auto socket = co_await connect_socket_async(host, port, cancel_event);
	TransportWebSocket websocket(std::move(socket));
	websocket.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::client));
	boost::system::error_code error;
	auto host_header = make_websocket_host_header(host, port);
	co_await websocket.async_handshake(host_header, "/", asio::redirect_error(asio::use_awaitable, error));
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to complete websocket handshake", error);
	}
	co_return websocket;
}

asio::awaitable<asio::ip::tcp::socket> accept_socket_async(std::uint16_t port, std::atomic<std::uint16_t>* bound_port, CancelEvent& cancel_event) {
	auto executor = co_await asio::this_coro::executor;
	asio::ip::tcp::acceptor acceptor(executor);
	auto on_cancel = cancel_event.connect([&acceptor] {
		boost::system::error_code ignored;
		acceptor.cancel(ignored);
		acceptor.close(ignored);
	});
	boost::system::error_code error;

	acceptor.open(asio::ip::tcp::v6(), error);
	if (!error) {
		acceptor.set_option(asio::ip::v6_only(false), error);
		if (!error) {
			acceptor.bind({asio::ip::tcp::v6(), port}, error);
		}
	}

	if (error) {
		error.clear();
		acceptor = asio::ip::tcp::acceptor(executor);
		acceptor.open(asio::ip::tcp::v4(), error);
		if (error) {
			throw make_boost_error("failed to open IPv4 listener socket", error);
		}
		acceptor.bind({asio::ip::tcp::v4(), port}, error);
		if (error) {
			throw make_boost_error("failed to bind IPv4 listener socket", error);
		}
	}

	acceptor.listen(asio::socket_base::max_listen_connections, error);
	if (error) {
		throw make_boost_error("failed to listen on socket", error);
	}
	if (bound_port != nullptr) {
		bound_port->store(acceptor.local_endpoint().port(), std::memory_order_relaxed);
	}
	auto socket = co_await acceptor.async_accept(asio::redirect_error(asio::use_awaitable, error));
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to accept receiver connection", error);
	}
	co_return socket;
}

asio::awaitable<TransportWebSocket> accept_websocket_async(std::uint16_t port, std::atomic<std::uint16_t>* bound_port, CancelEvent& cancel_event) {
	auto socket = co_await accept_socket_async(port, bound_port, cancel_event);
	TransportWebSocket websocket(std::move(socket));
	websocket.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
	boost::system::error_code error;
	co_await websocket.async_accept(asio::redirect_error(asio::use_awaitable, error));
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to accept websocket handshake", error);
	}
	co_return websocket;
}

void receive_transport_from_source(
	SocketByteSource& source,
	const std::filesystem::path& destination_dir,
	const std::shared_ptr<TransferProgress>& progress,
	CancelEvent& cancel_event) {
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<std::uint64_t> files_processed{0};
	auto config = make_runtime_config();

	RuntimeExecutors executors(config.worker_threads);
	BufferPool pool;
	auto write_budget = std::make_shared<InFlightWriteBudget>(config.max_in_flight_write_bytes);
	write_budget->listenCancelSignal(cancel_event);
	BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth, executors.executor());
	tar_queue.listenCancelSignal(cancel_event);
	PipelineState state;
	TarUnpacker unpacker(
		destination_dir,
		pool,
		executors,
		write_budget,
		config.max_in_flight_write_ops,
		config.max_parallel_extract_files);
	print_receive_progress_legend();
	auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed);

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[receive] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("n/u", tar_queue.size(), tar_queue.capacity()) << '}'
				<< " wb=" << format_scaled_bytes(write_budget->used_bytes(), "")
				<< '/' << format_scaled_bytes(write_budget->max_bytes(), "");
			return out.str();
		},
		uncompressed_bytes_processed);

	std::jthread input_thread([&] {
		try {
			ZstdDecompressor decompressor(pool);
			decompressor.decompress(source, tar_queue, &cancel_event);
		} catch (...) {
			state.fail(std::current_exception());
			tar_queue.close();
		}
	});

	std::jthread unpacker_thread([&] {
		try {
			unpacker.unpack(tar_queue, &uncompressed_bytes_processed, &files_processed, &cancel_event);
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
	progress_sync_thread.request_stop();
	if (progress_sync_thread.joinable()) {
		progress_sync_thread.join();
	}
	finalize_progress_line();
	state.rethrow_if_failed();
}

asio::awaitable<void> listen_directory_task(
	const std::filesystem::path source_dir,
	std::uint16_t port,
	RuntimeOptions options,
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint16_t>* bound_port,
	CancelEvent& cancel_event,
	std::exception_ptr* task_error) {
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<std::uint64_t> files_processed{0};
	try {
		set_progress_status(progress, "binding listener");
		auto websocket = co_await accept_websocket_async(port, bound_port, cancel_event);
		set_progress_status(progress, "receiver connected");
		std::cerr << "listening on port " << websocket.next_layer().local_endpoint().port() << ", waiting for receiver...\n";
		std::cerr << "receiver connected, starting transfer...\n";
		SocketByteSink sink(std::move(websocket), true);
		sink.listenCancelSignal(cancel_event);
		sink.send_transport_begin();
		auto config = make_runtime_config(options);
		const auto compression_level = options.compression_level.value_or(kDefaultCompressionLevel);
		const auto enable_adaptive_compression = !options.compression_level.has_value();

		RuntimeExecutors executors(config.worker_threads);
		BufferPool pool;
		auto read_budget = std::make_shared<InFlightReadBudget>(config.max_in_flight_read_bytes);
		read_budget->listenCancelSignal(cancel_event);
		BoundedQueue<OpenedFileReader> opened_queue(std::max(kOpenedQueueDepth, config.file_open_concurrency), executors.executor());
		BoundedQueue<OpenedFileReader> prefetched_queue(kPrefetchQueueDepth, executors.executor());
		BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth, executors.executor());
		BoundedQueue<DataChunk> zstd_queue(config.tar_queue_depth, executors.executor());
		opened_queue.listenCancelSignal(cancel_event);
		prefetched_queue.listenCancelSignal(cancel_event);
		tar_queue.listenCancelSignal(cancel_event);
		zstd_queue.listenCancelSignal(cancel_event);
		CompressionQueueTelemetry compression_telemetry{&tar_queue, &zstd_queue};
		DirScanner scanner(pool, executors, config.file_open_concurrency, kPipelineChunkSize, &cancel_event);
		FileReaderPrefetcher prefetcher(executors, read_budget, kPipelineChunkSize, &cancel_event);
		TarPacker packer(pool, kPipelineChunkSize);
		PipelineState state;
		std::atomic<int> active_compression_level{compression_level};
		print_listen_progress_legend();
		auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed);

		auto progress_thread = start_progress_thread(
			[&](std::uint64_t bytes_per_second) {
				std::ostringstream out;
				out << "[listen] " << format_rate(bytes_per_second)
					<< " q{" << queue_usage("s/p", opened_queue.size(), opened_queue.capacity())
					<< ' ' << queue_usage("p/t", prefetched_queue.size(), prefetched_queue.capacity())
					<< ' ' << queue_usage("t/z", tar_queue.size(), tar_queue.capacity())
					<< ' ' << queue_usage("z/n", zstd_queue.size(), zstd_queue.capacity())
					<< '}'
					<< " lvl=" << active_compression_level.load(std::memory_order_relaxed)
					<< " rb=" << format_scaled_bytes(read_budget->used_bytes(), "")
					<< '/' << format_scaled_bytes(read_budget->max_bytes(), "");
				return out.str();
			},
			uncompressed_bytes_processed);

		auto scanner_future = asio::co_spawn(executors.executor(), scanner.scan(source_dir, opened_queue), asio::use_future);
		auto prefetch_future = asio::co_spawn(executors.executor(), prefetcher.prefetch(opened_queue, prefetched_queue), asio::use_future);

		std::jthread sender_thread([&] {
			try {
				packer.pack(prefetched_queue, tar_queue, &uncompressed_bytes_processed, &files_processed, &cancel_event);
			} catch (...) {
				state.fail(std::current_exception());
				opened_queue.close();
				prefetched_queue.close();
				tar_queue.close();
				zstd_queue.close();
			}
		});

		std::jthread compressor_thread([&] {
			try {
				ZstdCompressor compressor(
					pool,
					executors,
					compression_level,
					enable_adaptive_compression ? &compression_telemetry : nullptr,
					&active_compression_level,
					options.log_adaptive_compression);
				compressor.compress(tar_queue, zstd_queue, &cancel_event);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
				zstd_queue.close();
			}
		});

		std::jthread sink_thread([&] {
			try {
				QueueWriter writer;
				writer.write(zstd_queue, sink, &cancel_event);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
				zstd_queue.close();
			}
		});

		try {
			scanner_future.get();
			prefetch_future.get();
		} catch (...) {
			state.fail(std::current_exception());
			opened_queue.close();
			prefetched_queue.close();
			tar_queue.close();
			zstd_queue.close();
		}
		join_and_capture(sender_thread, state);
		join_and_capture(compressor_thread, state);
		join_and_capture(sink_thread, state);
		progress_thread.request_stop();
		if (progress_thread.joinable()) {
			progress_thread.join();
		}
		progress_sync_thread.request_stop();
		if (progress_sync_thread.joinable()) {
			progress_sync_thread.join();
		}
		finalize_progress_line();
		state.rethrow_if_failed();
		sink.send_transport_end();
		sink.check_connection();
		sink.close_socket();
		complete_progress(progress, false, false, "send completed");
	} catch (...) {
		*task_error = std::current_exception();
		if (should_report_transfer_error(cancel_event, *task_error)) {
			complete_progress(progress, true, false, "send failed");
		} else if (is_transfer_cancelled(*task_error) || cancel_event.is_cancelled()) {
			complete_progress(progress, false, true, "cancelled");
		}
	}

	co_return;
}

asio::awaitable<void> receive_directory_task(
	std::string host,
	std::uint16_t port,
	const std::filesystem::path destination_dir,
	const std::shared_ptr<TransferProgress>& progress,
	bool keep_connection_open,
	CancelEvent& cancel_event,
	std::exception_ptr* task_error) {
	try {
		set_progress_status(progress, "connecting");
		std::cerr << "connecting to " << host << ':' << port << "...\n";
		auto websocket = co_await connect_websocket_async(std::move(host), port, cancel_event);
		std::cerr << "connected, waiting for transfer...\n";
		SocketByteSource source(std::move(websocket));
		source.listenCancelSignal(cancel_event);
		bool received_any_transport = false;
		for (;;) {
			if (received_any_transport) {
				set_progress_status(progress, "waiting for next transfer");
			}
			if (!source.await_transport_begin()) {
				if (keep_connection_open) {
					set_progress_status(progress, "waiting");
					break;
				}
				if (!received_any_transport) {
					throw std::runtime_error("sender closed websocket before starting transport");
				}
				break;
			}
			if (progress) {
				progress->reset("receiving");
			}
			std::cerr << "transport started, receiving...\n";
			receive_transport_from_source(source, destination_dir, progress, cancel_event);
			received_any_transport = true;
			complete_progress(progress, false, false, "receive completed");
			if (!keep_connection_open) {
				break;
			}
		}
		source.close_socket();
	} catch (...) {
		*task_error = std::current_exception();
		if (should_report_transfer_error(cancel_event, *task_error)) {
			complete_progress(progress, true, false, "receive failed");
		} else if (is_transfer_cancelled(*task_error) || cancel_event.is_cancelled()) {
			complete_progress(progress, false, true, "cancelled");
		}
	}

	co_return;
}

} // namespace

void pack_directory_to_file(const std::filesystem::path& source_dir, const std::filesystem::path& output_file, CompressionMode mode, RuntimeOptions options, CancelEvent* cancel_event) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	auto config = make_runtime_config(options);
	const auto compression_level = options.compression_level.value_or(kDefaultCompressionLevel);
	const auto enable_adaptive_compression = mode == CompressionMode::Zstd && !options.compression_level.has_value();
	RuntimeExecutors executors(config.worker_threads);
	BufferPool pool;
	auto read_budget = std::make_shared<InFlightReadBudget>(config.max_in_flight_read_bytes);
	read_budget->listenCancelSignal(effective_cancel_event);
	BoundedQueue<OpenedFileReader> opened_queue(std::max(kOpenedQueueDepth, config.file_open_concurrency), executors.executor());
	BoundedQueue<OpenedFileReader> prefetched_queue(kPrefetchQueueDepth, executors.executor());
	BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth, executors.executor());
	BoundedQueue<DataChunk> zstd_queue(config.tar_queue_depth, executors.executor());
	opened_queue.listenCancelSignal(effective_cancel_event);
	prefetched_queue.listenCancelSignal(effective_cancel_event);
	tar_queue.listenCancelSignal(effective_cancel_event);
	zstd_queue.listenCancelSignal(effective_cancel_event);
	CompressionQueueTelemetry compression_telemetry{&tar_queue, &zstd_queue};
	DirScanner scanner(pool, executors, config.file_open_concurrency, kPipelineChunkSize, &effective_cancel_event);
	FileReaderPrefetcher prefetcher(executors, read_budget, kPipelineChunkSize, &effective_cancel_event);
	TarPacker packer(pool, kPipelineChunkSize);
	PipelineState state;
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<int> active_compression_level{compression_level};
	print_pack_progress_legend(mode);

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[pack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/p", opened_queue.size(), opened_queue.capacity())
				<< ' ' << queue_usage("p/t", prefetched_queue.size(), prefetched_queue.capacity());
			if (mode == CompressionMode::Zstd) {
				out << ' ' << queue_usage("t/z", tar_queue.size(), tar_queue.capacity())
					<< ' ' << queue_usage("z/s", zstd_queue.size(), zstd_queue.capacity())
					<< '}'
					<< " lvl=" << active_compression_level.load(std::memory_order_relaxed);
			} else {
				out << ' ' << queue_usage("t/s", tar_queue.size(), tar_queue.capacity()) << '}';
			}
			out
				<< " rb=" << format_scaled_bytes(read_budget->used_bytes(), "")
				<< '/' << format_scaled_bytes(read_budget->max_bytes(), "");
			return out.str();
		},
		uncompressed_bytes_processed);

	FileByteSink sink(output_file, config.max_in_flight_write_ops);
	sink.listenCancelSignal(effective_cancel_event);
	auto scanner_future = asio::co_spawn(executors.executor(), scanner.scan(source_dir, opened_queue), asio::use_future);
	auto prefetch_future = asio::co_spawn(executors.executor(), prefetcher.prefetch(opened_queue, prefetched_queue), asio::use_future);

	std::jthread writer_thread([&] {
		try {
			packer.pack(prefetched_queue, tar_queue, &uncompressed_bytes_processed, nullptr, &effective_cancel_event);
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
			opened_queue.close();
			prefetched_queue.close();
			tar_queue.close();
			zstd_queue.close();
		}
	});

	std::jthread compression_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				ZstdCompressor compressor(
					pool,
					executors,
					compression_level,
					enable_adaptive_compression ? &compression_telemetry : nullptr,
					&active_compression_level,
					options.log_adaptive_compression);
				compressor.compress(tar_queue, zstd_queue, &effective_cancel_event);
			} else {
				QueueWriter writer;
				writer.write(tar_queue, sink, &effective_cancel_event);
			}
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
			tar_queue.close();
			zstd_queue.close();
		}
	});

	std::jthread sink_thread([&] {
		if (mode != CompressionMode::Zstd) {
			return;
		}
		try {
			QueueWriter writer;
			writer.write(zstd_queue, sink, &effective_cancel_event);
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
			tar_queue.close();
			zstd_queue.close();
		}
	});

	try {
		scanner_future.get();
		prefetch_future.get();
	} catch (...) {
		if (!is_transfer_cancelled(std::current_exception())) {
			state.fail(std::current_exception());
		}
		opened_queue.close();
		prefetched_queue.close();
		tar_queue.close();
		zstd_queue.close();
	}
	join_and_capture(writer_thread, state);
	join_and_capture(compression_thread, state);
	join_and_capture(sink_thread, state);
	progress_thread.request_stop();
	if (progress_thread.joinable()) {
		progress_thread.join();
	}
	finalize_progress_line();
	state.rethrow_if_failed();
	if (effective_cancel_event.is_cancelled()) {
		throw CancelledError("transfer cancelled");
	}
}

void unpack_file_to_directory(const std::filesystem::path& input_file, const std::filesystem::path& destination_dir, CompressionMode mode, RuntimeOptions options, CancelEvent* cancel_event) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	auto config = make_runtime_config(options);
	RuntimeExecutors executors(config.worker_threads);
	BufferPool pool;
	auto write_budget = std::make_shared<InFlightWriteBudget>(config.max_in_flight_write_bytes);
	write_budget->listenCancelSignal(effective_cancel_event);
	BoundedQueue<DataChunk> tar_queue(config.tar_queue_depth, executors.executor());
	tar_queue.listenCancelSignal(effective_cancel_event);
	PipelineState state;
	TarUnpacker unpacker(
		destination_dir,
		pool,
		executors,
		write_budget,
		config.max_in_flight_write_ops,
		config.max_parallel_extract_files);
	FileByteSource source(input_file);
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	print_unpack_progress_legend();

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[unpack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/u", tar_queue.size(), tar_queue.capacity()) << '}'
				<< " wb=" << format_scaled_bytes(write_budget->used_bytes(), "")
				<< '/' << format_scaled_bytes(write_budget->max_bytes(), "");
			return out.str();
		},
		uncompressed_bytes_processed);

	std::jthread input_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				ZstdDecompressor decompressor(pool);
				decompressor.decompress(source, tar_queue, &effective_cancel_event);
			} else {
				RawTarReader reader(pool);
				reader.read(source, tar_queue, &effective_cancel_event);
			}
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
			tar_queue.close();
		}
	});

	std::jthread unpacker_thread([&] {
		try {
			unpacker.unpack(tar_queue, &uncompressed_bytes_processed, nullptr, &effective_cancel_event);
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
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
	if (effective_cancel_event.is_cancelled()) {
		throw CancelledError("transfer cancelled");
	}
}

void listen_directory(const std::filesystem::path& source_dir, std::uint16_t port, RuntimeOptions options) {
	listen_directory(source_dir, port, options, {}, nullptr, {}, nullptr);
}

void listen_directory(
	const std::filesystem::path& source_dir,
	std::uint16_t port,
	RuntimeOptions options,
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint16_t>* bound_port,
	std::stop_token stop_token,
	CancelEvent* cancel_event) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	std::stop_callback on_stop(stop_token, [&effective_cancel_event] {
		effective_cancel_event.emit();
	});
	asio::io_context io_context;
	std::exception_ptr task_error;
	asio::co_spawn(io_context, listen_directory_task(source_dir, port, options, progress, bound_port, effective_cancel_event, &task_error), asio::detached);
	io_context.run();
	if (task_error && should_report_transfer_error(effective_cancel_event, task_error)) {
		std::rethrow_exception(task_error);
	}
}

void receive_directory(std::string_view host, std::uint16_t port, const std::filesystem::path& destination_dir) {
	receive_directory(host, port, destination_dir, {}, {}, nullptr, false);
}

void receive_directory(
	std::string_view host,
	std::uint16_t port,
	const std::filesystem::path& destination_dir,
	const std::shared_ptr<TransferProgress>& progress,
	std::stop_token stop_token,
	CancelEvent* cancel_event,
	bool keep_connection_open) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	std::stop_callback on_stop(stop_token, [&effective_cancel_event] {
		effective_cancel_event.emit();
	});
	asio::io_context io_context;
	std::exception_ptr task_error;
	asio::co_spawn(io_context, receive_directory_task(std::string(host), port, destination_dir, progress, keep_connection_open, effective_cancel_event, &task_error), asio::detached);
	io_context.run();
	if (task_error && should_report_transfer_error(effective_cancel_event, task_error)) {
		std::rethrow_exception(task_error);
	}
}

} // namespace soratransport
