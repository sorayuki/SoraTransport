#include "gui_runtime.hpp"

#include "internal.hpp"
#include "win32_util.hpp"

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

void close_transport_socket(asio::ip::tcp::socket& socket) {
	boost::system::error_code ignored;
	socket.cancel(ignored);
	socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
	socket.close(ignored);
}

void close_transport_socket(TransportWebSocket& websocket) {
	close_transport_socket(websocket.next_layer());
}

template <typename Rep, typename Period>
bool wait_for_stop_or_timeout(std::stop_token stop_token, std::chrono::duration<Rep, Period> timeout) {
	std::mutex mutex;
	std::condition_variable cv;
	std::atomic<bool> stop_requested{false};
	std::stop_callback on_stop(stop_token, [&] {
		stop_requested.store(true, std::memory_order_release);
		cv.notify_all();
	});
	std::unique_lock lock(mutex);
	return cv.wait_for(lock, timeout, [&] {
		return stop_requested.load(std::memory_order_acquire);
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

std::jthread start_progress_sync_thread(
	const std::shared_ptr<TransferProgress>& progress,
	std::atomic<std::uint64_t>& processed_bytes,
	std::atomic<std::uint64_t>& processed_files) {
	return std::jthread([progress, &processed_bytes, &processed_files](std::stop_token stop_token) {
		using namespace std::chrono_literals;
		std::uint64_t previous_bytes = 0;
		std::uint64_t previous_files = 0;
		while (!stop_token.stop_requested()) {
			if (wait_for_stop_or_timeout(stop_token, 250ms)) {
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
	return cancel_event.is_cancelled()
		&& (error == asio::error::operation_aborted
			|| error == asio::error::bad_descriptor
			|| error == asio::error::connection_reset
			|| error == asio::error::eof
			|| error == boost::beast::websocket::error::closed);
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
	CancelEvent& cancel_event) {
	std::atomic<std::uint64_t> uncompressed_bytes_processed{0};
	std::atomic<std::uint64_t> files_processed{0};

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
	auto progress_sync_thread = start_progress_sync_thread(progress, uncompressed_bytes_processed, files_processed);

	auto scanner_future = asio::co_spawn(executors.executor(), scanner.scan(source_paths, opened_queue), asio::use_future);
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
	State(std::shared_ptr<TransferProgress> progress, RuntimeOptions options)
		: progress_(std::move(progress)), options_(std::move(options)) {}

	~State() {
		stop();
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
	void run(std::stop_token stop_token) {
		try {
			if (progress_) {
				progress_->reset("binding listener");
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

			if (progress_) {
				progress_->set_status("waiting for receiver");
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
				if (progress_) {
					progress_->set_status("receiver connected, waiting for drop");
				}

				bool receiver_connected = true;
				while (receiver_connected && !stop_token.stop_requested() && !cancel_event_.is_cancelled()) {
					std::vector<std::filesystem::path> source_paths;
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
							transfer_in_progress_ = true;
						}
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
								progress_->set_cancelled("cancelled");
							}
						}
						continue;
					}

					try {
						if (progress_) {
							progress_->reset("sending");
						}
						send_paths_to_socket(source_paths, sink, options_, progress_, cancel_event_);
						if (progress_) {
							progress_->set_completed("send completed");
							progress_->set_status("receiver connected, waiting for drop");
						}
					} catch (...) {
						receiver_connected = false;
						if (progress_) {
							try {
								throw;
							} catch (const CancelledError&) {
								progress_->set_cancelled("cancelled");
							} catch (const std::exception& error) {
								progress_->set_failed(error.what());
							} catch (...) {
								progress_->set_failed("send failed");
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
				}

				sink.close_socket();
				{
					std::lock_guard lock(mutex_);
					receiver_connected_ = false;
					transfer_in_progress_ = false;
					pending_paths_.reset();
				}

				if (!stop_token.stop_requested() && !cancel_event_.is_cancelled() && progress_) {
					progress_->set_status("waiting for receiver");
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
					progress_->set_failed(error.what());
				} catch (...) {
					progress_->set_failed("send failed");
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
};

GuiSendServer::GuiSendServer(const std::shared_ptr<TransferProgress>& progress, RuntimeOptions options)
	: state_(std::make_unique<State>(progress, std::move(options))) {}

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

} // namespace soratransport