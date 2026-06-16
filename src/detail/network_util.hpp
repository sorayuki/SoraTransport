#pragma once

#include "pipeline.hpp"
#include "win32_util.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/websocket/stream.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <stop_token>

namespace soratransport {

// TransportWebSocket is defined in io.hpp (included via pipeline.hpp)

// ---------------------------------------------------------------------------
// make_boost_error  — 将 Boost 错误码映射为异常消息。
// 在 Windows 上 system_category 即为 Win32 错误码域，可安全取 FormatMessage 文本。
// ---------------------------------------------------------------------------
inline std::runtime_error make_boost_error(const std::string& message, const boost::system::error_code& error) {
	// system_category on Windows maps to Win32 error codes via GetLastError
	if (error.category() == boost::system::system_category()) {
		return std::runtime_error(message + ": " + win32_error_message_utf8(static_cast<DWORD>(error.value())));
	}
	return std::runtime_error(message + ": " + error.message());
}

// ---------------------------------------------------------------------------
// 传输层 socket / websocket 关闭
// ---------------------------------------------------------------------------
inline void close_transport_socket(boost::asio::ip::tcp::socket& socket) {
	boost::system::error_code ignored;
	socket.cancel(ignored);
	socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
	socket.close(ignored);
}

inline void close_transport_socket(TransportWebSocket& websocket) {
	close_transport_socket(websocket.next_layer());
}

// ---------------------------------------------------------------------------
// wait_for_stop_or_timeout  — 等待 stop_token 或 cancel_event 触发或超时。
// 返回 true 表示应退出。
// ---------------------------------------------------------------------------
template <typename Rep, typename Period>
bool wait_for_stop_or_timeout(
	std::stop_token stop_token,
	const CancelEvent* cancel_event,
	std::chrono::duration<Rep, Period> timeout) {
	if (stop_token.stop_requested() || (cancel_event != nullptr && cancel_event->is_cancelled())) {
		return true;
	}

	constexpr auto kStopWaitPollInterval = std::chrono::milliseconds(50);
	const auto deadline = std::chrono::steady_clock::now()
		+ std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
	const auto poll_interval = std::chrono::duration_cast<std::chrono::steady_clock::duration>(kStopWaitPollInterval);

	std::mutex mutex;
	std::condition_variable cv;
	std::atomic<bool> stop_requested{false};
	std::stop_callback on_stop(stop_token, [&] {
		stop_requested.store(true, std::memory_order_release);
		cv.notify_all();
	});
	std::unique_lock lock(mutex);
	for (;;) {
		if (stop_requested.load(std::memory_order_acquire)
			|| (cancel_event != nullptr && cancel_event->is_cancelled())) {
			return true;
		}

		const auto now = std::chrono::steady_clock::now();
		if (now >= deadline) {
			return false;
		}

		const auto remaining = deadline - now;
		const auto wait_slice = remaining < poll_interval ? remaining : poll_interval;
		cv.wait_for(lock, wait_slice, [&] {
			return stop_requested.load(std::memory_order_acquire);
		});
	}
}

// ---------------------------------------------------------------------------
// 异步取消错误判断
// ---------------------------------------------------------------------------
inline bool is_transfer_cancelled(const std::exception_ptr& error) {
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

inline bool should_throw_cancelled_error(const CancelEvent& cancel_event, const boost::system::error_code& error) {
	namespace asio = boost::asio;
	return cancel_event.is_cancelled()
		&& (error == asio::error::operation_aborted
			|| error == asio::error::bad_descriptor
			|| error == asio::error::connection_reset
			|| error == asio::error::eof
			|| error == boost::beast::websocket::error::closed);
}

inline bool should_report_transfer_error(const CancelEvent& cancel_event, const std::exception_ptr& error) {
	return !cancel_event.is_cancelled() && !is_transfer_cancelled(error);
}

} // namespace soratransport
