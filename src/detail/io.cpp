#include "io.hpp"
#include "win32_util.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kBufferedWriteBatchSize = 8 * 1024 * 1024;
constexpr std::size_t kSocketIoBufferSize = 1 * 1024 * 1024;
constexpr std::size_t kSocketIoReceiveBufferSize = kSocketIoBufferSize / 2;
constexpr auto kSocketIoMaxBufferedSendDelay = std::chrono::milliseconds(100);

std::runtime_error make_socket_error(std::string_view action, const boost::system::error_code& error) {
	if (error.category() == boost::system::system_category()) {
		return std::runtime_error(std::string(action) + ": " + win32_error_message_utf8(static_cast<DWORD>(error.value())));
	}
	return std::runtime_error(std::string(action) + ": " + error.message());
}

bool is_cancelled_socket_error(bool cancel_requested, const boost::system::error_code& error) {
	using boost::asio::error::bad_descriptor;
	using boost::asio::error::operation_aborted;
	return cancel_requested && (error == operation_aborted || error == bad_descriptor);
}

std::size_t read_some(HANDLE handle, uint8_t* buffer, std::size_t length, const std::string& path) {
	std::size_t total_bytes_read = 0;
	while (total_bytes_read < length) {
		const auto remaining = length - total_bytes_read;
		const auto chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
		DWORD bytes_read = 0;
		if (!::ReadFile(handle, buffer + total_bytes_read, chunk, &bytes_read, nullptr)) {
			throw make_win32_error("failed to read input file: " + path);
		}
		if (bytes_read == 0) {
			break;
		}
		total_bytes_read += bytes_read;
		if (bytes_read < chunk) {
			break;
		}
	}
	return total_bytes_read;
}

void flush_file_buffers_if_supported(HANDLE handle, const std::string& path, std::string_view action) {
	if (::FlushFileBuffers(handle)) {
		return;
	}

	const auto error = ::GetLastError();
	if (error == ERROR_INVALID_FUNCTION && ::GetFileType(handle) != FILE_TYPE_DISK) {
		return;
	}

	throw make_win32_error(std::string(action) + ": " + path, error);
}

} // namespace

struct FileByteSink::State {
	struct WriteSlot : OverlappedSlotBase {
		std::size_t size = 0;
		std::uint64_t offset = 0;
	};

	HANDLE handle = INVALID_HANDLE_VALUE;
	std::string display_path;
	bool closed = false;
	std::size_t write_buffer_capacity = 0;
	std::size_t max_in_flight_write_ops = 1;
	std::uint64_t physical_size = 0;
	std::vector<WriteSlot> write_slots;
	std::deque<std::size_t> available_slots;
	std::deque<std::size_t> in_flight_slots;
	std::size_t active_slot_index = 0;
	std::atomic<bool> cancel_requested{false};
	boost::signals2::scoped_connection cancel_connection;
};

struct FileByteSource::State {
	HANDLE handle = INVALID_HANDLE_VALUE;
	std::string display_path;
};

FileByteSink::FileByteSink(const std::filesystem::path& output_path, std::size_t max_in_flight_write_ops) : state_(std::make_unique<State>()) {
	state_->display_path = path_to_utf8_string(output_path);
	state_->max_in_flight_write_ops = std::max<std::size_t>(1, max_in_flight_write_ops);
	const auto flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_OVERLAPPED;
	state_->handle = ::CreateFileW(
		output_path.c_str(),
		GENERIC_WRITE,
		0,
		nullptr,
		CREATE_ALWAYS,
		flags,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open output file: " + state_->display_path);
	}
	state_->write_slots.reserve(state_->max_in_flight_write_ops + 1);
	for (std::size_t index = 0; index < state_->max_in_flight_write_ops + 1; ++index) {
		state_->write_slots.emplace_back();
	}
	state_->write_buffer_capacity = kBufferedWriteBatchSize;
	for (auto& slot : state_->write_slots) {
		slot.buffer = make_heap_buffer(state_->write_buffer_capacity);
	}
	state_->active_slot_index = 0;
	for (std::size_t index = 1; index < state_->write_slots.size(); ++index) {
		state_->available_slots.push_back(index);
	}
}

FileByteSink::~FileByteSink() {
	if (state_ && state_->handle != INVALID_HANDLE_VALUE) {
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}
}

void FileByteSink::listenCancelSignal(CancelEvent& event) {
	if (!state_) {
		return;
	}
	state_->cancel_connection = event.connect([this] {
		cancel_pending_work();
	});
}

void FileByteSink::write(std::span<const uint8_t> bytes) {
	if (!state_ || state_->closed || state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("output file is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("output file write cancelled");
	}
	while (!bytes.empty()) {
		auto& active_slot = state_->write_slots[state_->active_slot_index];
		if (active_slot.size == state_->write_buffer_capacity) {
			submit_active_write();
		}

		const auto available = state_->write_buffer_capacity - active_slot.size;
		const auto chunk = std::min<std::size_t>(available, bytes.size());
		std::memcpy(active_slot.buffer.get() + active_slot.size, bytes.data(), chunk);
		active_slot.size += chunk;
		bytes = bytes.subspan(chunk);

		if (active_slot.size == state_->write_buffer_capacity) {
			submit_active_write();
		}
	}
}

void FileByteSink::submit_active_write() {
	if (!state_) {
		return;
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("output file write cancelled");
	}

	auto& slot = state_->write_slots[state_->active_slot_index];
	if (slot.size == 0) {
		return;
	}
	if (slot.in_flight) {
		throw std::runtime_error("write slot is unexpectedly still in flight");
	}

	if (state_->in_flight_slots.size() >= state_->max_in_flight_write_ops) {
		wait_for_one_write();
	}

	slot.offset = state_->physical_size;
	slot.overlapped = {};
	slot.overlapped.Offset = static_cast<DWORD>(slot.offset & 0xffffffffull);
	slot.overlapped.OffsetHigh = static_cast<DWORD>((slot.offset >> 32) & 0xffffffffull);
	slot.overlapped.hEvent = slot.event_handle;
	::ResetEvent(slot.event_handle);

	const auto ok = ::WriteFile(
		state_->handle,
		slot.buffer.get(),
		static_cast<DWORD>(slot.size),
		nullptr,
		&slot.overlapped);
	if (!ok && ::GetLastError() != ERROR_IO_PENDING) {
		if (state_->cancel_requested.load(std::memory_order_acquire)) {
			throw CancelledError("output file write cancelled");
		}
		throw make_win32_error("failed to write output file: " + state_->display_path);
	}

	slot.in_flight = true;
	state_->physical_size += slot.size;
	state_->in_flight_slots.push_back(state_->active_slot_index);

	if (state_->available_slots.empty()) {
		wait_for_one_write();
	}
	if (state_->available_slots.empty()) {
		throw std::runtime_error("no write slot available after waiting for completion");
	}

	state_->active_slot_index = state_->available_slots.front();
	state_->available_slots.pop_front();
	state_->write_slots[state_->active_slot_index].size = 0;
}

void FileByteSink::wait_for_one_write() {
	if (!state_ || state_->in_flight_slots.empty()) {
		return;
	}

	const auto slot_index = state_->in_flight_slots.front();
	state_->in_flight_slots.pop_front();
	auto& slot = state_->write_slots[slot_index];
	DWORD bytes_written = 0;
	if (!::GetOverlappedResult(state_->handle, &slot.overlapped, &bytes_written, TRUE)) {
		const auto error = ::GetLastError();
		slot.in_flight = false;
		slot.size = 0;
		if (state_->cancel_requested.load(std::memory_order_acquire)
			&& (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED)) {
			throw CancelledError("output file write cancelled");
		}
		throw make_win32_error("failed to complete output write: " + state_->display_path, error);
	}
	if (bytes_written != slot.size) {
		slot.in_flight = false;
		slot.size = 0;
		throw std::runtime_error("unexpected short write to output file: " + state_->display_path);
	}
	slot.in_flight = false;
	slot.size = 0;
	slot.offset = 0;
	state_->available_slots.push_back(slot_index);
}

void FileByteSink::wait_for_all_writes() {
	while (state_ && !state_->in_flight_slots.empty()) {
		wait_for_one_write();
	}
}

void FileByteSink::close() {
	if (!state_ || state_->closed) {
		return;
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("output file write cancelled");
	}
	submit_active_write();
	wait_for_all_writes();
	state_->closed = true;
	try {
		flush_file_buffers_if_supported(state_->handle, state_->display_path, "failed to flush output file");
	} catch (...) {
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw;
	}
	if (!::CloseHandle(state_->handle)) {
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to close output file: " + state_->display_path);
	}
	state_->handle = INVALID_HANDLE_VALUE;
}

bool FileByteSink::is_cancelled() const {
	return state_ != nullptr && state_->cancel_requested.load(std::memory_order_acquire);
}

void FileByteSink::cancel_pending_work() {
	if (!state_) {
		return;
	}
	state_->cancel_requested.store(true, std::memory_order_release);
	if (state_->handle != INVALID_HANDLE_VALUE) {
		::CancelIoEx(state_->handle, nullptr);
	}
}

FileByteSource::FileByteSource(const std::filesystem::path& input_path) : state_(std::make_unique<State>()) {
	state_->display_path = path_to_utf8_string(input_path);
	state_->handle = ::CreateFileW(
		input_path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open input file: " + state_->display_path);
	}
}

FileByteSource::~FileByteSource() {
	if (state_ && state_->handle != INVALID_HANDLE_VALUE) {
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}
}

std::size_t FileByteSource::read(uint8_t* buffer, std::size_t length) {
	if (!state_ || state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("input file is closed");
	}
	if (length == 0) {
		return 0;
	}
	return read_some(state_->handle, buffer, length, state_->display_path);
}

struct SocketByteSink::State {
	explicit State(boost::asio::ip::tcp::socket s)
		: socket(std::move(s)), io_context(static_cast<boost::asio::io_context&>(socket.get_executor().context())) {
		for (auto& buffer : buffers) {
			buffer.resize(kSocketIoBufferSize);
		}
	}

	~State() {
		stop();
	}

	void write(std::span<const uint8_t> bytes) {
		rethrow_async_error();
		if (closed) {
			throw std::runtime_error("socket sink is closed");
		}
		if (cancel_requested) {
			throw CancelledError("socket send cancelled");
		}

		std::size_t offset = 0;
		while (offset < bytes.size()) {
			poll_pending_send();
			rethrow_async_error();
			auto& fill_size = sizes[fill_index];
			const auto available = kSocketIoBufferSize - fill_size;
			if (available == 0) {
				submit_fill_buffer();
				continue;
			}

			const auto was_empty = fill_size == 0;
			const auto now = std::chrono::steady_clock::now();
			const auto chunk = std::min<std::size_t>(available, bytes.size() - offset);
			std::memcpy(buffers[fill_index].data() + fill_size, bytes.data() + offset, chunk);
			fill_size += chunk;
			offset += chunk;
			if (was_empty) {
				fill_started_at = now;
			}
			if (fill_size == kSocketIoBufferSize
				|| (fill_started_at.has_value() && now - *fill_started_at >= kSocketIoMaxBufferedSendDelay)) {
				submit_fill_buffer();
			}
		}
	}

	void close() {
		if (closed) {
			rethrow_async_error();
			return;
		}
		if (cancel_requested) {
			throw CancelledError("socket send cancelled");
		}
		rethrow_async_error();
		submit_fill_buffer();
		wait_for_pending_send();
		closed = true;
		shutdown_socket();
		rethrow_async_error();
	}

	void stop() {
		if (stop_completed) {
			return;
		}
		cancel_pending_work();
		if (send_in_flight) {
			wait_for_pending_send();
		}
		stop_completed = true;
	}

	void close_socket() {
		if (stop_requested || cancel_requested) {
			return;
		}
		stop_requested = true;
		if (send_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
		}
		shutdown_socket();
	}

	void submit_fill_buffer() {
		if (sizes[fill_index] == 0) {
			return;
		}
		if (cancel_requested) {
			throw CancelledError("socket send cancelled");
		}
		wait_for_pending_send();
		rethrow_async_error();

		const auto submit_index = fill_index;
		send_in_flight = true;
		send_completed = false;
		completed_index.reset();
		send_error.clear();
		boost::asio::async_write(
			socket,
			boost::asio::buffer(buffers[submit_index].data(), sizes[submit_index]),
			[this, submit_index](const boost::system::error_code& error, std::size_t bytes_transferred) {
				send_in_flight = false;
				send_completed = true;
				send_error = error;
				send_bytes_transferred = bytes_transferred;
				completed_index = submit_index;
			});

		fill_index = 1 - fill_index;
		if (sizes[fill_index] != 0) {
			throw std::runtime_error("socket sink fill buffer was not released");
		}
		fill_started_at.reset();
		poll_pending_send();
	}

	void poll_pending_send() {
		if (!send_in_flight) {
			finalize_send_if_ready();
			return;
		}
		poll_io();
		finalize_send_if_ready();
	}

	void wait_for_pending_send() {
		while (send_in_flight) {
			run_one_io();
			finalize_send_if_ready();
		}
		finalize_send_if_ready();
	}

	void finalize_send_if_ready() {
		if (!send_completed) {
			return;
		}
		send_completed = false;
		if (send_error) {
			if (!async_error) {
				if (is_cancelled_socket_error(cancel_requested, send_error)) {
					async_error = std::make_exception_ptr(CancelledError("socket send cancelled"));
				} else {
					async_error = std::make_exception_ptr(make_socket_error("socket write failed", send_error));
				}
			}
		} else if (completed_index.has_value()) {
			const auto index = *completed_index;
			if (send_bytes_transferred != sizes[index] && !async_error) {
				async_error = std::make_exception_ptr(std::runtime_error("socket write completed with short transfer"));
			}
			sizes[index] = 0;
		}
		completed_index.reset();
		send_error.clear();
		send_bytes_transferred = 0;
	}

	void shutdown_socket() {
		if (socket_shutdown) {
			return;
		}
		socket_shutdown = true;
		boost::system::error_code ignored;
		socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ignored);
		socket.close(ignored);
	}

	void poll_io() {
		io_context.restart();
		while (io_context.poll_one() > 0) {
		}
	}

	void run_one_io() {
		io_context.restart();
		io_context.run_one();
	}

	void rethrow_async_error() const {
		if (async_error) {
			std::rethrow_exception(async_error);
		}
	}

	void cancel_pending_work() {
		if (cancel_requested) {
			return;
		}
		cancel_requested = true;
		if (send_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
		}
		shutdown_socket();
	}

	boost::asio::ip::tcp::socket socket;
	boost::asio::io_context& io_context;
	std::array<std::vector<uint8_t>, 2> buffers;
	std::array<std::size_t, 2> sizes{};
	std::size_t fill_index = 0;
	std::optional<std::chrono::steady_clock::time_point> fill_started_at;
	std::optional<std::size_t> completed_index;
	bool closed = false;
	bool stop_requested = false;
	bool stop_completed = false;
	bool send_in_flight = false;
	bool send_completed = false;
	std::size_t send_bytes_transferred = 0;
	boost::system::error_code send_error;
	std::exception_ptr async_error;
	bool socket_shutdown = false;
	boost::signals2::scoped_connection cancel_connection;
	bool cancel_requested = false;
};

SocketByteSink::SocketByteSink(boost::asio::ip::tcp::socket socket) : state_(std::make_unique<State>(std::move(socket))) {}

SocketByteSink::~SocketByteSink() {
	if (state_) {
		state_->stop();
	}
}

SocketByteSink::SocketByteSink(SocketByteSink&&) noexcept = default;

SocketByteSink& SocketByteSink::operator=(SocketByteSink&&) noexcept = default;

void SocketByteSink::listenCancelSignal(CancelEvent& event) {
	if (state_) {
		state_->cancel_connection = event.connect([this] {
			cancel_pending_work();
		});
	}
}

void SocketByteSink::write(std::span<const uint8_t> bytes) {
	state_->write(bytes);
}

void SocketByteSink::close() {
	state_->close();
}

void SocketByteSink::close_socket() {
	if (state_) {
		state_->close_socket();
	}
}

void SocketByteSink::stop() {
	if (state_) {
		state_->stop();
	}
}

bool SocketByteSink::is_cancelled() const {
	return state_ != nullptr && state_->cancel_requested;
}

void SocketByteSink::cancel_pending_work() {
	if (state_) {
		state_->cancel_pending_work();
	}
}

struct SocketByteSource::State {
	explicit State(boost::asio::ip::tcp::socket s)
		: socket(std::move(s)), io_context(static_cast<boost::asio::io_context&>(socket.get_executor().context())) {
		active_buffer.resize(kSocketIoBufferSize);
		staged_buffer.resize(kSocketIoBufferSize);
		receive_buffer.resize(kSocketIoReceiveBufferSize);
		start_receive_if_possible();
	}

	~State() {
		stop();
	}

	std::size_t read(uint8_t* output, std::size_t length) {
		if (length == 0) {
			return 0;
		}

		rethrow_async_error();
		if (cancel_requested) {
			throw CancelledError("socket receive cancelled");
		}
		std::size_t copied_total = 0;
		for (;;) {
			promote_staged_buffer_if_possible();
			while (active_offset < active_size && copied_total < length) {
				const auto available = active_size - active_offset;
				const auto chunk = std::min<std::size_t>(available, length - copied_total);
				std::memcpy(output + copied_total, active_buffer.data() + active_offset, chunk);
				active_offset += chunk;
				copied_total += chunk;
			}

			if (active_offset == active_size) {
				active_size = 0;
				active_offset = 0;
				promote_staged_buffer_if_possible();
				start_receive_if_possible();
			}

			if (copied_total == length) {
				start_receive_if_possible();
				poll_io();
				return copied_total;
			}
			if (copied_total > 0) {
				start_receive_if_possible();
				poll_io();
				return copied_total;
			}
			rethrow_async_error();
			if (eof) {
				return 0;
			}
			wait_for_ready_data();
		}
	}

	void stop() {
		if (stop_completed) {
			return;
		}
		cancel_pending_work();
		if (receive_in_flight) {
			while (receive_in_flight) {
				run_one_io();
			}
		}
		stop_completed = true;
	}

	void close_socket() {
		if (stop_requested || cancel_requested) {
			return;
		}
		stop_requested = true;
		if (receive_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
		}
		shutdown_socket();
	}

	void start_receive_if_possible() {
		if (stop_requested || cancel_requested || eof || async_error || receive_in_flight) {
			return;
		}
		if (staged_free_capacity() < receive_buffer.size()) {
			return;
		}

		receive_in_flight = true;
		boost::asio::async_read(
			socket,
			boost::asio::buffer(receive_buffer.data(), receive_buffer.size()),
			boost::asio::transfer_at_least(1),
			[this](const boost::system::error_code& error, std::size_t bytes_transferred) {
				receive_in_flight = false;

				if (error == boost::asio::error::eof || bytes_transferred == 0) {
					eof = true;
					return;
				}
				if (error) {
					if (is_cancelled_socket_error(cancel_requested, error)) {
						async_error = std::make_exception_ptr(CancelledError("socket receive cancelled"));
					} else {
						async_error = std::make_exception_ptr(make_socket_error("socket read failed", error));
					}
					return;
				}

				if (bytes_transferred > staged_free_capacity()) {
					async_error = std::make_exception_ptr(std::runtime_error("socket receive staging buffer overflow"));
					return;
				}

				std::memcpy(staged_buffer.data() + staged_size, receive_buffer.data(), bytes_transferred);
				staged_size += bytes_transferred;
				start_receive_if_possible();
			});
	}

	void wait_for_ready_data() {
		while (!has_readable_data() && !eof && !async_error && !cancel_requested) {
			start_receive_if_possible();
			run_one_io();
		}
		promote_staged_buffer_if_possible();
	}

	bool has_readable_data() const {
		return active_offset < active_size || staged_size > 0;
	}

	std::size_t staged_free_capacity() const {
		return staged_buffer.size() - staged_size;
	}

	void promote_staged_buffer_if_possible() {
		if (active_offset < active_size || staged_size == 0) {
			return;
		}
		active_buffer.swap(staged_buffer);
		active_size = staged_size;
		active_offset = 0;
		staged_size = 0;
	}

	void shutdown_socket() {
		if (socket_shutdown) {
			return;
		}
		socket_shutdown = true;
		boost::system::error_code ignored;
		socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
		socket.close(ignored);
	}

	void poll_io() {
		io_context.restart();
		while (io_context.poll_one() > 0) {
		}
	}

	void run_one_io() {
		io_context.restart();
		io_context.run_one();
	}

	void rethrow_async_error() const {
		if (async_error) {
			std::rethrow_exception(async_error);
		}
	}

	void cancel_pending_work() {
		if (cancel_requested) {
			return;
		}
		cancel_requested = true;
		if (receive_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
		}
		shutdown_socket();
	}

	boost::asio::ip::tcp::socket socket;
	boost::asio::io_context& io_context;
	std::vector<uint8_t> active_buffer;
	std::vector<uint8_t> staged_buffer;
	std::vector<uint8_t> receive_buffer;
	std::size_t active_size = 0;
	std::size_t active_offset = 0;
	std::size_t staged_size = 0;
	bool eof = false;
	bool stop_requested = false;
	bool stop_completed = false;
	bool receive_in_flight = false;
	std::exception_ptr async_error;
	bool socket_shutdown = false;
	boost::signals2::scoped_connection cancel_connection;
	bool cancel_requested = false;
};

SocketByteSource::SocketByteSource(boost::asio::ip::tcp::socket socket) : state_(std::make_unique<State>(std::move(socket))) {}

SocketByteSource::~SocketByteSource() {
	if (state_) {
		state_->stop();
	}
}

SocketByteSource::SocketByteSource(SocketByteSource&&) noexcept = default;

SocketByteSource& SocketByteSource::operator=(SocketByteSource&&) noexcept = default;

void SocketByteSource::listenCancelSignal(CancelEvent& event) {
	if (state_) {
		state_->cancel_connection = event.connect([this] {
			cancel_pending_work();
		});
	}
}

std::size_t SocketByteSource::read(uint8_t* buffer, std::size_t length) {
	return state_->read(buffer, length);
}

void SocketByteSource::close_socket() {
	if (state_) {
		state_->close_socket();
	}
}

void SocketByteSource::stop() {
	if (state_) {
		state_->stop();
	}
}

bool SocketByteSource::is_cancelled() const {
	return state_ != nullptr && state_->cancel_requested;
}

void SocketByteSource::cancel_pending_work() {
	if (state_) {
		state_->cancel_pending_work();
	}
}

} // namespace soratransport
