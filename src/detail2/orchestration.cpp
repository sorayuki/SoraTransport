#include "../core.hpp"

#include "config.hpp"
#include "file_compare_stream.hpp"
#include "filesystem.hpp"
#include "protocol.hpp"
#include "stream.hpp"
#include "tar.hpp"
#include "zstd.hpp"

#include "../detail/internal.hpp"
#include "../detail/network_util.hpp"
#include "../detail/win32_util.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stop_token>
#include <vector>

namespace asio = boost::asio;

namespace soratransport {

namespace {

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
			<< "  q{s/o o/p p/t t/z z/s}: queue usage current/capacity\n"
			<< "    s/o = traverser -> opener\n"
			<< "    o/p = opener -> prefetcher\n"
			<< "    p/t = prefetcher -> tar packer\n"
			<< "    t/z = tar packer -> zstd compressor\n"
			<< "    z/s = zstd compressor -> sink writer\n"
			<< "  lvl: active zstd compression level\n";
	} else {
		std::cerr
			<< "  q{s/o o/p p/t t/s}: queue usage current/capacity\n"
			<< "    s/o = traverser -> opener\n"
			<< "    o/p = opener -> prefetcher\n"
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
		<< "  q{s/o o/p p/t t/z z/n}: queue usage current/capacity\n"
		<< "    s/o = traverser -> opener\n"
		<< "    o/p = opener -> prefetcher\n"
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

std::jthread start_progress_thread(
	std::function<std::string(std::uint64_t)> build_line,
	std::atomic<std::uint64_t>& processed_bytes,
	const CancelEvent* cancel_event = nullptr) {
	return std::jthread([build_line = std::move(build_line), &processed_bytes, cancel_event](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous = processed_bytes.load(std::memory_order_relaxed);
		while (!stop_token.stop_requested()) {
			if (wait_for_stop_or_timeout(stop_token, cancel_event, 1s)) {
				break;
			}
			const auto current = processed_bytes.load(std::memory_order_relaxed);
			const auto delta = current - previous;
			previous = current;
			print_progress_line(build_line(delta));
		}
	});
}

void set_progress_status(const std::shared_ptr<TransferProgress>& progress, StatusText status) {
	if (progress) {
		progress->set_status(std::move(status));
	}
}

void complete_progress(const std::shared_ptr<TransferProgress>& progress, bool failed, bool cancelled, StatusText status) {
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
	std::atomic<std::uint64_t>& processed_files,
	const CancelEvent* cancel_event = nullptr) {
	return std::jthread([progress, &processed_bytes, &processed_files, cancel_event](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous_bytes = 0;
		std::uint64_t previous_files = 0;
		while (!stop_token.stop_requested()) {
			if (wait_for_stop_or_timeout(stop_token, cancel_event, 250ms)) {
				break;
			}
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

asio::awaitable<asio::ip::tcp::socket> connect_socket_async(std::string host, std::uint16_t port, CancelEvent& cancel_event) {
	auto executor = co_await asio::this_coro::executor;
	asio::ip::tcp::resolver resolver(executor);
	asio::ip::tcp::socket socket(executor);
	auto on_cancel = cancel_event.connect([&resolver, &socket] {
		resolver.cancel();
		close_transport_socket(socket);
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
	auto on_cancel = cancel_event.connect([&websocket] {
		close_transport_socket(websocket);
	});
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
	auto on_cancel = cancel_event.connect([&websocket] {
		close_transport_socket(websocket);
	});
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
	auto tuning = detail2::make_pipeline_tuning();

	detail2::TaskExecutor executor(tuning.worker_threads);
	BufferPool pool;
	BoundedQueue<DataChunk> tar_queue(tuning.tar_queue_capacity, executor.executor());
	tar_queue.listenCancelSignal(cancel_event);
	PipelineState state;
	detail2::TarUnpacker unpacker(
		destination_dir,
		pool,
		executor,
		tuning,
		&cancel_event);
	print_receive_progress_legend();
	auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed, &cancel_event);

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[receive] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("n/u", tar_queue.size(), tar_queue.capacity()) << '}';
			return out.str();
		},
		uncompressed_bytes_processed,
		&cancel_event);

	std::jthread input_thread([&] {
		try {
			detail2::ZstdDecompressor decompressor(pool);
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
	std::exception_ptr* task_error,
	bool enable_file_comparison = false) {
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<std::uint64_t> files_processed{0};
	try {
		set_progress_status(progress, {"binding listener", "正在绑定监听端口"});
		auto websocket = co_await accept_websocket_async(port, bound_port, cancel_event);
		set_progress_status(progress, {"receiver connected", "接收端已连接，等待拖放"});
		std::cerr << "listening on port " << websocket.next_layer().local_endpoint().port() << ", waiting for receiver...\n";
		std::cerr << "receiver connected, starting transfer...\n";
		SocketByteSink sink(std::move(websocket), true);
		sink.listenCancelSignal(cancel_event);
		sink.send_transport_begin(enable_file_comparison);

		auto tuning = detail2::make_pipeline_tuning(options);
		const auto compression_level = options.compression_level.value_or(tuning.default_compression_level);
		const auto enable_adaptive_compression = !options.compression_level.has_value();

		detail2::TaskExecutor executors(tuning.worker_threads);
		BufferPool pool;
		detail2::SemaphoreCor tar_budget(executors.executor(), tuning.tar_output_budget_bytes);
		detail2::SemaphoreCor zstd_budget(executors.executor(), tuning.zstd_output_budget_bytes);
		BoundedQueue<detail2::TraversalEntry> traversal_queue(tuning.opened_queue_capacity, executors.executor());
		BoundedQueue<detail2::TraversalEntry> filtered_traversal_queue(tuning.opened_queue_capacity, executors.executor());
		BoundedQueue<detail2::OpenedFile> opened_queue(tuning.opened_queue_capacity, executors.executor());
		BoundedQueue<detail2::OpenedFile> prefetched_queue(tuning.prefetched_queue_capacity, executors.executor());
		BoundedQueue<DataChunk> tar_queue(tuning.tar_queue_capacity, executors.executor());
		BoundedQueue<DataChunk> zstd_queue(tuning.zstd_queue_capacity, executors.executor());
		traversal_queue.listenCancelSignal(cancel_event);
		filtered_traversal_queue.listenCancelSignal(cancel_event);
		opened_queue.listenCancelSignal(cancel_event);
		prefetched_queue.listenCancelSignal(cancel_event);
		tar_queue.listenCancelSignal(cancel_event);
		zstd_queue.listenCancelSignal(cancel_event);
		CompressionQueueTelemetry compression_telemetry{&tar_queue, &zstd_queue};
		detail2::FileTraverser traverser(executors.executor(), tuning, &cancel_event);
		detail2::FileOpener opener(pool, executors, tuning, &cancel_event);
		detail2::FilePrefetcher prefetcher(executors.executor(), tuning, &cancel_event);
		detail2::TarPacker packer(pool, tuning.pipeline_chunk_size, &tar_budget);
		PipelineState state;
		std::atomic<int> active_compression_level{compression_level};
		print_listen_progress_legend();
		auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed, &cancel_event);

		auto progress_thread = start_progress_thread(
			[&](std::uint64_t bytes_per_second) {
				std::ostringstream out;
				out << "[listen] " << format_rate(bytes_per_second)
					<< " q{" << queue_usage("s/o", traversal_queue.size(), traversal_queue.capacity())
					<< ' ' << queue_usage("o/p", opened_queue.size(), opened_queue.capacity())
					<< ' ' << queue_usage("p/t", prefetched_queue.size(), prefetched_queue.capacity())
					<< ' ' << queue_usage("t/z", tar_queue.size(), tar_queue.capacity())
					<< ' ' << queue_usage("z/n", zstd_queue.size(), zstd_queue.capacity())
					<< '}'
					<< " lvl=" << active_compression_level.load(std::memory_order_relaxed)
					<< " rb=" << format_scaled_bytes(prefetcher.used_budget_bytes(), "")
					<< '/' << format_scaled_bytes(prefetcher.total_budget_bytes(), "");
				return out.str();
			},
			uncompressed_bytes_processed,
			&cancel_event);

		std::future<void> traverse_future;
		std::future<void> open_future;
		std::future<void> prefetch_future;
		bool traverse_already_consumed = false;

		std::jthread sender_thread([&] {
			PipelineGuard guard;
			guard.watch(traversal_queue);
			guard.watch(filtered_traversal_queue);
			guard.watch(opened_queue);
			guard.watch(prefetched_queue);
			guard.watch(tar_queue);
			guard.watch(zstd_queue);
			try {
				packer.pack(prefetched_queue, tar_queue, &uncompressed_bytes_processed, &files_processed, &cancel_event);
				guard.dismiss();
			} catch (...) {
				state.fail(std::current_exception());
			}
		});

		std::jthread compressor_thread([&] {
			try {
				{
					detail2::ZstdCompressor compressor(
						pool,
						tuning.worker_threads,
						compression_level,
						&zstd_budget,
						enable_adaptive_compression ? &compression_telemetry : nullptr,
						&active_compression_level,
						options.log_adaptive_compression);
					compressor.compress(tar_queue, zstd_queue, &cancel_event);
				}
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
				zstd_queue.close();
			}
		});

		std::jthread sink_thread([&] {
			try {
				detail2::QueueWriter writer;
				writer.write(zstd_queue, sink, &cancel_event);
			} catch (...) {
				state.fail(std::current_exception());
				tar_queue.close();
				zstd_queue.close();
			}
		});

		if (enable_file_comparison) {
			// ================================================================
			// 文件比较模式：FileTraverser → 流式控制通道交换 → FileOpener
			// ================================================================

			open_future = asio::co_spawn(
				executors.executor(),
				opener.open(filtered_traversal_queue, opened_queue),
				asio::use_future);
			prefetch_future = asio::co_spawn(
				executors.executor(),
				prefetcher.prefetch(opened_queue, prefetched_queue),
				asio::use_future);

			// Phase 1: 遍历目录，收集所有 TraversalEntry
			traverse_future = asio::co_spawn(
				executors.executor(),
				traverser.traverse(source_dir, traversal_queue),
				asio::use_future);

			detail2::stream_file_comparison_to_opener(
				traversal_queue,
				filtered_traversal_queue,
				sink,
				cancel_event);
			traverse_future.get();
			traverse_already_consumed = true;
		} else {
			// ================================================================
			// 直连模式：FileTraverser → FileOpener 直接连接（原有行为）
			// ================================================================
			traverse_future = asio::co_spawn(
				executors.executor(),
				traverser.traverse(source_dir, traversal_queue),
				asio::use_future);
			open_future = asio::co_spawn(
				executors.executor(),
				opener.open(traversal_queue, opened_queue),
				asio::use_future);
			prefetch_future = asio::co_spawn(
				executors.executor(),
				prefetcher.prefetch(opened_queue, prefetched_queue),
				asio::use_future);
		}

		try {
			if (!traverse_already_consumed) {
				traverse_future.get();
			}
			open_future.get();
			prefetch_future.get();
		} catch (...) {
			state.fail(std::current_exception());
			PipelineGuard guard;
			guard.watch(traversal_queue);
			guard.watch(opened_queue);
			guard.watch(prefetched_queue);
			guard.watch(tar_queue);
			guard.watch(zstd_queue);
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
		complete_progress(progress, false, false, {"send completed", "发送完成"});
	} catch (...) {
		*task_error = std::current_exception();
		if (should_report_transfer_error(cancel_event, *task_error)) {
			complete_progress(progress, true, false, {"send failed", "发送失败"});
		} else if (is_transfer_cancelled(*task_error) || cancel_event.is_cancelled()) {
			complete_progress(progress, false, true, {"cancelled", "已取消"});
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
		set_progress_status(progress, {"connecting", "正在连接"});
		std::cerr << "connecting to " << host << ':' << port << "...\n";
		auto websocket = co_await connect_websocket_async(std::move(host), port, cancel_event);
		std::cerr << "connected, waiting for transfer...\n";
		SocketByteSource source(std::move(websocket));
		source.listenCancelSignal(cancel_event);
		bool received_any_transport = false;
		for (;;) {
			if (received_any_transport) {
				set_progress_status(progress, {"waiting for next transfer", "等待下一次传输"});
			}
			if (!source.await_transport_begin()) {
				if (keep_connection_open) {
					set_progress_status(progress, {"waiting", "等待操作"});
					break;
				}
				if (!received_any_transport) {
					throw std::runtime_error("sender closed websocket before starting transport");
				}
				break;
			}

			// ================================================================
			// 文件比较交换（如果发送端启用了 file_comparison）
			// ================================================================
			std::unique_ptr<detail2::TaskExecutor> compare_executor;
			if (source.transport_has_file_comparison()) {
				std::cerr << "file comparison enabled, processing streamed file info...\n";
				auto tuning = detail2::make_pipeline_tuning();
				compare_executor = std::make_unique<detail2::TaskExecutor>(tuning.worker_threads);
				source.set_data_control_message_handler(
					[&source, &destination_dir, executor = compare_executor.get()](std::string_view control_json) {
						detail2::handle_file_comparison_control_message(
							control_json,
							destination_dir,
							source,
							*executor);
					});
			}

			if (progress) {
				progress->reset({"receiving", "正在接收"});
			}
			std::cerr << "transport started, receiving...\n";
			receive_transport_from_source(source, destination_dir, progress, cancel_event);
			source.set_data_control_message_handler({});
			received_any_transport = true;
			complete_progress(progress, false, false, {"receive completed", "接收完成"});
			if (!keep_connection_open) {
				break;
			}
		}
		source.close_socket();
	} catch (...) {
		*task_error = std::current_exception();
		if (should_report_transfer_error(cancel_event, *task_error)) {
			complete_progress(progress, true, false, {"receive failed", "接收失败"});
		} else if (is_transfer_cancelled(*task_error) || cancel_event.is_cancelled()) {
			complete_progress(progress, false, true, {"cancelled", "已取消"});
		}
	}

	co_return;
}

} // namespace

void pack_directory_to_file(
	const std::filesystem::path& source_dir,
	const std::filesystem::path& output_file,
	CompressionMode mode,
	RuntimeOptions options,
	CancelEvent* cancel_event) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	auto tuning = detail2::make_pipeline_tuning(options);
	const auto compression_level = options.compression_level.value_or(tuning.default_compression_level);
	const auto enable_adaptive_compression = mode == CompressionMode::Zstd && !options.compression_level.has_value();
	detail2::TaskExecutor executors(tuning.worker_threads);
	BufferPool pool;
	detail2::SemaphoreCor tar_budget(executors.executor(), tuning.tar_output_budget_bytes);
	detail2::SemaphoreCor zstd_budget(executors.executor(), tuning.zstd_output_budget_bytes);
	BoundedQueue<detail2::TraversalEntry> traversal_queue(tuning.opened_queue_capacity, executors.executor());
	BoundedQueue<detail2::OpenedFile> opened_queue(tuning.opened_queue_capacity, executors.executor());
	BoundedQueue<detail2::OpenedFile> prefetched_queue(tuning.prefetched_queue_capacity, executors.executor());
	BoundedQueue<DataChunk> tar_queue(tuning.tar_queue_capacity, executors.executor());
	BoundedQueue<DataChunk> zstd_queue(tuning.zstd_queue_capacity, executors.executor());
	traversal_queue.listenCancelSignal(effective_cancel_event);
	opened_queue.listenCancelSignal(effective_cancel_event);
	prefetched_queue.listenCancelSignal(effective_cancel_event);
	tar_queue.listenCancelSignal(effective_cancel_event);
	zstd_queue.listenCancelSignal(effective_cancel_event);
	CompressionQueueTelemetry compression_telemetry{&tar_queue, &zstd_queue};
	detail2::FileTraverser traverser(executors.executor(), tuning, &effective_cancel_event);
	detail2::FileOpener opener(pool, executors, tuning, &effective_cancel_event);
	detail2::FilePrefetcher prefetcher(executors.executor(), tuning, &effective_cancel_event);
	detail2::TarPacker packer(pool, tuning.pipeline_chunk_size, &tar_budget);
	PipelineState state;
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<int> active_compression_level{compression_level};
	print_pack_progress_legend(mode);

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[pack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/o", traversal_queue.size(), traversal_queue.capacity())
				<< ' ' << queue_usage("o/p", opened_queue.size(), opened_queue.capacity())
				<< ' ' << queue_usage("p/t", prefetched_queue.size(), prefetched_queue.capacity());
			if (mode == CompressionMode::Zstd) {
				out << ' ' << queue_usage("t/z", tar_queue.size(), tar_queue.capacity())
					<< ' ' << queue_usage("z/s", zstd_queue.size(), zstd_queue.capacity())
					<< '}'
					<< " lvl=" << active_compression_level.load(std::memory_order_relaxed);
			} else {
				out << ' ' << queue_usage("t/s", tar_queue.size(), tar_queue.capacity()) << '}';
			}
			out << " rb=" << format_scaled_bytes(prefetcher.used_budget_bytes(), "")
				<< '/' << format_scaled_bytes(prefetcher.total_budget_bytes(), "");
			return out.str();
		},
		uncompressed_bytes_processed,
		&effective_cancel_event);

	FileByteSink sink(output_file, tuning.max_in_flight_write_ops);
	sink.listenCancelSignal(effective_cancel_event);
	auto traverse_future = asio::co_spawn(executors.executor(), traverser.traverse(source_dir, traversal_queue), asio::use_future);
	auto open_future = asio::co_spawn(executors.executor(), opener.open(traversal_queue, opened_queue), asio::use_future);
	auto prefetch_future = asio::co_spawn(executors.executor(), prefetcher.prefetch(opened_queue, prefetched_queue), asio::use_future);

	std::jthread writer_thread([&] {
		PipelineGuard guard;
		guard.watch(traversal_queue);
		guard.watch(opened_queue);
		guard.watch(prefetched_queue);
		guard.watch(tar_queue);
		guard.watch(zstd_queue);
		try {
			packer.pack(prefetched_queue, tar_queue, &uncompressed_bytes_processed, nullptr, &effective_cancel_event);
			guard.dismiss();
		} catch (...) {
			if (!is_transfer_cancelled(std::current_exception())) {
				state.fail(std::current_exception());
			}
		}
	});

	std::jthread compression_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				detail2::ZstdCompressor compressor(
					pool,
					tuning.worker_threads,
					compression_level,
					&zstd_budget,
					enable_adaptive_compression ? &compression_telemetry : nullptr,
					&active_compression_level,
					options.log_adaptive_compression);
				compressor.compress(tar_queue, zstd_queue, &effective_cancel_event);
			} else {
				detail2::QueueWriter writer;
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
			detail2::QueueWriter writer;
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
		traverse_future.get();
		open_future.get();
		prefetch_future.get();
	} catch (...) {
		if (!is_transfer_cancelled(std::current_exception())) {
			state.fail(std::current_exception());
		}
		PipelineGuard guard;
		guard.watch(traversal_queue);
		guard.watch(opened_queue);
		guard.watch(prefetched_queue);
		guard.watch(tar_queue);
		guard.watch(zstd_queue);
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

void unpack_file_to_directory(
	const std::filesystem::path& input_file,
	const std::filesystem::path& destination_dir,
	CompressionMode mode,
	RuntimeOptions options,
	CancelEvent* cancel_event) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	auto tuning = detail2::make_pipeline_tuning(options);
	detail2::TaskExecutor executor(tuning.worker_threads);
	BufferPool pool;
	BoundedQueue<DataChunk> tar_queue(tuning.tar_queue_capacity, executor.executor());
	tar_queue.listenCancelSignal(effective_cancel_event);
	PipelineState state;
	detail2::TarUnpacker unpacker(
		destination_dir,
		pool,
		executor,
		tuning,
		&effective_cancel_event);
	FileByteSource source(input_file);
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	print_unpack_progress_legend();

	auto progress_thread = start_progress_thread(
		[&](std::uint64_t bytes_per_second) {
			std::ostringstream out;
			out << "[unpack] " << format_rate(bytes_per_second)
				<< " q{" << queue_usage("s/u", tar_queue.size(), tar_queue.capacity()) << '}';
			return out.str();
		},
		uncompressed_bytes_processed,
		&effective_cancel_event);

	std::jthread input_thread([&] {
		try {
			if (mode == CompressionMode::Zstd) {
				detail2::ZstdDecompressor decompressor(pool);
				decompressor.decompress(source, tar_queue, &effective_cancel_event);
			} else {
				detail2::RawTarReader reader(pool);
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

void listen_directory(
	const std::filesystem::path& source_dir,
	std::uint16_t port,
	RuntimeOptions options,
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint16_t>* bound_port,
	std::stop_token stop_token,
	CancelEvent* cancel_event,
	bool enable_file_comparison) {
	CancelEvent local_cancel_event;
	CancelEvent& effective_cancel_event = cancel_event != nullptr ? *cancel_event : local_cancel_event;
	std::stop_callback on_stop(stop_token, [&effective_cancel_event] {
		effective_cancel_event.emit();
	});
	asio::io_context io_context;
	std::exception_ptr task_error;
	asio::co_spawn(io_context, listen_directory_task(source_dir, port, options, progress, bound_port, effective_cancel_event, &task_error, enable_file_comparison), asio::detached);
	io_context.run();
	if (task_error && should_report_transfer_error(effective_cancel_event, task_error)) {
		std::rethrow_exception(task_error);
	}
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
