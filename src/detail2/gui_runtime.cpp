#include "gui_runtime.hpp"

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

#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_future.hpp>

#include <condition_variable>
#include <mutex>
#include <sstream>

namespace asio = boost::asio;

namespace soratransport {

namespace {

std::jthread start_progress_sync_thread(
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint64_t>& processed_bytes,
	std::atomic<std::uint64_t>& processed_files) {
	return std::jthread([progress, &processed_bytes, &processed_files](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous_bytes = 0;
		std::uint64_t previous_files = 0;
		while (!stop_token.stop_requested()) {
			if (wait_for_stop_or_timeout(stop_token, nullptr, 250ms)) {
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

asio::ip::tcp::acceptor make_listener_acceptor(asio::io_context& io_context, std::uint16_t port) {
	auto executor = io_context.get_executor();
	asio::ip::tcp::acceptor acceptor(executor);
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
	return acceptor;
}

asio::ip::tcp::socket accept_socket(asio::io_context& io_context, asio::ip::tcp::acceptor& acceptor, CancelEvent& cancel_event) {
	boost::system::error_code error;
	std::optional<asio::ip::tcp::socket> accepted_socket;
	bool completed = false;
	auto on_cancel = cancel_event.connect([&acceptor] {
		boost::system::error_code ignored;
		acceptor.cancel(ignored);
		acceptor.close(ignored);
	});

	acceptor.async_accept([&](const boost::system::error_code& input_error, asio::ip::tcp::socket socket) mutable {
		error = input_error;
		if (!input_error) {
			accepted_socket.emplace(std::move(socket));
		}
		completed = true;
	});

	io_context.restart();
	while (!completed) {
		io_context.run_one();
	}

	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to accept receiver connection", error);
	}
	return std::move(*accepted_socket);
}

TransportWebSocket accept_websocket(asio::io_context& io_context, asio::ip::tcp::acceptor& acceptor, CancelEvent& cancel_event) {
	auto socket = accept_socket(io_context, acceptor, cancel_event);
	TransportWebSocket websocket(std::move(socket));
	websocket.set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
	auto on_cancel = cancel_event.connect([&websocket] {
		close_transport_socket(websocket);
	});
	boost::system::error_code error;
	websocket.accept(error);
	if (error) {
		if (should_throw_cancelled_error(cancel_event, error)) {
			throw CancelledError("transfer cancelled");
		}
		throw make_boost_error("failed to accept websocket handshake", error);
	}
	return websocket;
}

void send_paths_to_socket(
	const std::vector<std::filesystem::path>& source_paths,
	SocketByteSink& sink,
	RuntimeOptions options,
	const std::shared_ptr<TransferProgress>& progress,
	CancelEvent& cancel_event,
	bool enable_file_comparison = false) {
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<std::uint64_t> files_processed{0};

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
	auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed);

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
		PipelineGuard guard;
		guard.watch(tar_queue);
		guard.watch(zstd_queue);
		try {
			detail2::ZstdCompressor compressor(
				pool,
				tuning.worker_threads,
				compression_level,
				&zstd_budget,
				enable_adaptive_compression ? &compression_telemetry : nullptr,
				&active_compression_level,
				options.log_adaptive_compression);
			compressor.compress(tar_queue, zstd_queue, &cancel_event);
			guard.dismiss();
		} catch (...) {
			state.fail(std::current_exception());
		}
	});

	std::jthread sink_thread([&] {
		PipelineGuard guard;
		guard.watch(tar_queue);
		guard.watch(zstd_queue);
		try {
			detail2::QueueWriter writer;
			writer.write(zstd_queue, sink, &cancel_event);
			guard.dismiss();
		} catch (...) {
			state.fail(std::current_exception());
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
			traverser.traverse(source_paths, traversal_queue),
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
			traverser.traverse(source_paths, traversal_queue),
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
		guard.watch(filtered_traversal_queue);
		guard.watch(opened_queue);
		guard.watch(prefetched_queue);
		guard.watch(tar_queue);
		guard.watch(zstd_queue);
	}

	join_and_capture(sender_thread, state);
	join_and_capture(compressor_thread, state);
	join_and_capture(sink_thread, state);
	progress_sync_thread.request_stop();
	if (progress_sync_thread.joinable()) {
		progress_sync_thread.join();
	}
	state.rethrow_if_failed();
	sink.send_transport_end();
	sink.check_connection();
	if (cancel_event.is_cancelled()) {
		throw CancelledError("transfer cancelled");
	}
}

} // namespace

class GuiSendServer::State {
public:
	State(std::shared_ptr<TransferProgress> progress, RuntimeOptions options, std::function<void()> on_state_changed)
		: progress_(std::move(progress)), options_(std::move(options)),
		  on_state_changed_(std::move(on_state_changed)) {}

	~State() {
		stop();
	}

	void set_file_comparison(bool enabled) {
		std::lock_guard lock(mutex_);
		enable_file_comparison_ = enabled;
	}

	bool file_comparison() const {
		std::lock_guard lock(mutex_);
		return enable_file_comparison_;
	}

	void start() {
		std::unique_lock lock(mutex_);
		if (thread_.joinable()) {
			return;
		}
		startup_complete_ = false;
		startup_error_ = nullptr;
		thread_ = std::jthread([this](std::stop_token stop_token) {
			run(stop_token);
		});
		startup_cv_.wait(lock, [this] {
			return startup_complete_;
		});
		if (startup_error_) {
			std::rethrow_exception(startup_error_);
		}
	}

	void stop() {
		std::unique_lock lock(mutex_);
		if (!thread_.joinable()) {
			return;
		}
		cancel_event_.emit();
		pending_paths_.reset();
		cv_.notify_all();
		auto thread = std::move(thread_);
		lock.unlock();
		thread.request_stop();
		if (thread.joinable()) {
			thread.join();
		}
	}

	std::optional<std::string> submit_paths(std::vector<std::filesystem::path> paths) {
		if (paths.empty()) {
			return std::string("请至少放入一个文件或文件夹");
		}

		std::lock_guard lock(mutex_);
		if (!receiver_connected_) {
			return std::string("还没有接收端连接");
		}
		if (transfer_in_progress_ || pending_paths_.has_value()) {
			return std::string("当前发送任务尚未结束");
		}
		pending_paths_ = std::move(paths);
		cv_.notify_all();
		return std::nullopt;
	}

	GuiSendServerSnapshot snapshot() const {
		std::lock_guard lock(mutex_);
		return GuiSendServerSnapshot{
			.listening = listening_,
			.receiver_connected = receiver_connected_,
			.transfer_in_progress = transfer_in_progress_,
			.bound_port = bound_port_,
		};
	}

private:
	void notify_state_changed() {
		if (on_state_changed_) {
			on_state_changed_();
		}
	}

	void run(std::stop_token stop_token) {
		try {
			if (progress_) {
				progress_->reset({"binding listener", "正在绑定监听端口"});
			}

			asio::io_context io_context;
			auto acceptor = make_listener_acceptor(io_context, 0);

			{
				std::lock_guard lock(mutex_);
				bound_port_ = acceptor.local_endpoint().port();
				listening_ = true;
				startup_complete_ = true;
			}
			startup_cv_.notify_all();
			notify_state_changed();

			if (progress_) {
				progress_->set_status({"waiting for receiver", "等待接收端连接"});
			}

			while (!stop_token.stop_requested() && !cancel_event_.is_cancelled()) {
				auto websocket = accept_websocket(io_context, acceptor, cancel_event_);
				SocketByteSink sink(std::move(websocket), true);
				sink.listenCancelSignal(cancel_event_);
				{
					std::lock_guard lock(mutex_);
					receiver_connected_ = true;
					transfer_in_progress_ = false;
				}
				notify_state_changed();
				if (progress_) {
					progress_->set_status({"receiver connected, waiting for drop", "接收端已连接，等待拖放"});
				}

				bool receiver_connected = true;
				while (receiver_connected && !stop_token.stop_requested() && !cancel_event_.is_cancelled()) {
					std::vector<std::filesystem::path> source_paths;
					bool enable_file_comparison = false;
					{
						std::unique_lock lock(mutex_);
						const bool has_pending_paths = cv_.wait_for(lock, std::chrono::milliseconds(500), [&] {
							return stop_token.stop_requested() || cancel_event_.is_cancelled() || pending_paths_.has_value();
						});
						if (stop_token.stop_requested() || cancel_event_.is_cancelled()) {
							receiver_connected = false;
							break;
						}
						if (has_pending_paths) {
							source_paths = std::move(*pending_paths_);
							pending_paths_.reset();
							enable_file_comparison = enable_file_comparison_;
							transfer_in_progress_ = true;
						}
					}
					if (!source_paths.empty()) {
						notify_state_changed();
					}

					if (!receiver_connected) {
						break;
					}
					if (source_paths.empty()) {
						try {
							sink.check_connection();
						} catch (...) {
							receiver_connected = false;
							if (progress_ && cancel_event_.is_cancelled()) {
								progress_->set_cancelled({"cancelled", "已取消"});
							}
						}
						continue;
					}

					try {
						if (progress_) {
							progress_->reset({"sending", "正在发送"});
						}
						send_paths_to_socket(source_paths, sink, options_, progress_, cancel_event_, enable_file_comparison);
						if (progress_) {
							progress_->set_completed({"send completed", "发送完成"});
							progress_->set_status({"receiver connected, waiting for drop", "接收端已连接，等待拖放"});
						}
					} catch (...) {
						receiver_connected = false;
						if (progress_) {
							try {
								throw;
							} catch (const CancelledError&) {
								progress_->set_cancelled({"cancelled", "已取消"});
							} catch (const std::exception& error) {
								progress_->set_failed({error.what(), error.what()});
							} catch (...) {
								progress_->set_failed({"send failed", "发送失败"});
							}
						}
						if (cancel_event_.is_cancelled()) {
							break;
						}
					}

					{
						std::lock_guard lock(mutex_);
						transfer_in_progress_ = false;
					}
					notify_state_changed();
				}

				sink.close_socket();
				{
					std::lock_guard lock(mutex_);
					receiver_connected_ = false;
					transfer_in_progress_ = false;
					pending_paths_.reset();
				}
				notify_state_changed();

				if (!stop_token.stop_requested() && !cancel_event_.is_cancelled() && progress_) {
					progress_->set_status({"waiting for receiver", "等待接收端连接"});
				}
			}
		} catch (...) {
			{
				std::lock_guard lock(mutex_);
				startup_error_ = std::current_exception();
				startup_complete_ = true;
			}
			startup_cv_.notify_all();
			if (progress_) {
				try {
					std::rethrow_exception(startup_error_);
				} catch (const std::exception& error) {
					progress_->set_failed({error.what(), error.what()});
				} catch (...) {
					progress_->set_failed({"send failed", "发送失败"});
				}
			}
		}

		std::lock_guard lock(mutex_);
		listening_ = false;
		receiver_connected_ = false;
		transfer_in_progress_ = false;
		pending_paths_.reset();
	}

	std::shared_ptr<TransferProgress> progress_;
	RuntimeOptions options_;
	std::function<void()> on_state_changed_;
	mutable std::mutex mutex_;
	std::condition_variable cv_;
	std::condition_variable startup_cv_;
	std::jthread thread_;
	CancelEvent cancel_event_;
	std::optional<std::vector<std::filesystem::path>> pending_paths_;
	std::exception_ptr startup_error_;
	bool startup_complete_ = false;
	bool listening_ = false;
	bool receiver_connected_ = false;
	bool transfer_in_progress_ = false;
	std::uint16_t bound_port_ = 0;
	bool enable_file_comparison_ = false;
};

GuiSendServer::GuiSendServer(const std::shared_ptr<TransferProgress>& progress, RuntimeOptions options, std::function<void()> on_state_changed)
	: state_(std::make_unique<State>(progress, std::move(options), std::move(on_state_changed))) {}

GuiSendServer::~GuiSendServer() = default;

void GuiSendServer::start() {
	state_->start();
}

void GuiSendServer::stop() {
	state_->stop();
}

std::optional<std::string> GuiSendServer::submit_paths(std::vector<std::filesystem::path> paths) {
	return state_->submit_paths(std::move(paths));
}

GuiSendServerSnapshot GuiSendServer::snapshot() const {
	return state_->snapshot();
}

void GuiSendServer::set_file_comparison(bool enabled) {
	state_->set_file_comparison(enabled);
}

bool GuiSendServer::file_comparison() const {
	return state_->file_comparison();
}

} // namespace soratransport
