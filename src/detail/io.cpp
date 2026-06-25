#include "io.hpp"
#include "win32_util.hpp"

#include "../detail2/buffered_sender.hpp"
#include "../detail2/protocol.hpp"

#include <boost/asio/buffer.hpp>
#include <boost/beast/core/flat_buffer.hpp>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

constexpr std::size_t kBufferedWriteBatchSize = 8 * 1024 * 1024;
constexpr std::size_t kFileByteSourceChunkSize = 4 * 1024 * 1024;
constexpr std::size_t kSocketIoBufferSize = 1 * 1024 * 1024;
constexpr auto kSocketIoMaxBufferedSendDelay = std::chrono::milliseconds(100);
constexpr std::size_t kControlBufferCapacity = 256 * 1024;  // 控制通道缓冲 256KB
constexpr auto kSocketKeepaliveIdleInterval = std::chrono::seconds(15);
constexpr auto kSocketKeepalivePollInterval = std::chrono::seconds(5);
constexpr std::string_view kTransportBeginEvent = "transport_begin";
constexpr std::string_view kTransportEndEvent = "transport_end";
constexpr std::size_t kControlMessageLengthSize = 4;

std::runtime_error make_socket_error(std::string_view action, const boost::system::error_code& error) {
	if (error.category() == boost::system::system_category()) {
		return std::runtime_error(std::string(action) + ": " + win32_error_message_utf8(static_cast<DWORD>(error.value())));
	}
	return std::runtime_error(std::string(action) + ": " + error.message());
}

bool is_cancelled_socket_error(bool cancel_requested, const boost::system::error_code& error) {
	using boost::asio::error::bad_descriptor;
	using boost::asio::error::eof;
	using boost::asio::error::operation_aborted;
	return cancel_requested
		&& (error == operation_aborted
			|| error == bad_descriptor
			|| error == eof
			|| error == boost::beast::websocket::error::closed);
}

bool is_cancelled_win32_error(bool cancel_requested, DWORD error) {
	return cancel_requested && (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED);
}

std::uint64_t get_file_size(HANDLE handle, const std::string& display_path) {
	LARGE_INTEGER size{};
	if (!::GetFileSizeEx(handle, &size)) {
		throw make_win32_error("failed to query input file size: " + display_path);
	}
	return static_cast<std::uint64_t>(std::max<LONGLONG>(0, size.QuadPart));
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

std::int64_t steady_clock_millis() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now().time_since_epoch())
		.count();
}

std::string make_transport_event_payload(std::string_view event_id) {
	nlohmann::json payload = {
		{"type", "event"},
		{"event_id", event_id},
	};
	return payload.dump();
}

void append_control_message_record(std::vector<uint8_t>& out, std::string_view json_payload) {
	if (json_payload.size() > std::numeric_limits<std::uint32_t>::max()) {
		throw std::runtime_error("control message is too large");
	}
	const auto size = static_cast<std::uint32_t>(json_payload.size());
	out.push_back(static_cast<uint8_t>((size >> 24) & 0xff));
	out.push_back(static_cast<uint8_t>((size >> 16) & 0xff));
	out.push_back(static_cast<uint8_t>((size >> 8) & 0xff));
	out.push_back(static_cast<uint8_t>(size & 0xff));
	out.insert(out.end(), json_payload.begin(), json_payload.end());
}

std::vector<std::string> decode_control_messages(std::span<const uint8_t> payload) {
	if (payload.empty()) {
		return {};
	}

	std::vector<std::string> messages;
	std::size_t offset = 0;
	while (offset < payload.size()) {
		if (payload.size() - offset < kControlMessageLengthSize) {
			return {std::string(reinterpret_cast<const char*>(payload.data()), payload.size())};
		}

		const auto message_size =
			(static_cast<std::uint32_t>(payload[offset]) << 24)
			| (static_cast<std::uint32_t>(payload[offset + 1]) << 16)
			| (static_cast<std::uint32_t>(payload[offset + 2]) << 8)
			| static_cast<std::uint32_t>(payload[offset + 3]);
		offset += kControlMessageLengthSize;
		if (message_size == 0 || message_size > payload.size() - offset) {
			return {std::string(reinterpret_cast<const char*>(payload.data()), payload.size())};
		}

		messages.emplace_back(
			reinterpret_cast<const char*>(payload.data() + offset),
			message_size);
		offset += message_size;
	}
	return messages;
}

std::string parse_transport_event_payload(std::string_view payload_text) {
	try {
		auto payload = nlohmann::json::parse(payload_text);
		if (!payload.is_object()) {
			throw std::runtime_error("websocket control frame must be a JSON object");
		}
		const auto type_it = payload.find("type");
		const auto event_id_it = payload.find("event_id");
		if (type_it == payload.end() || !type_it->is_string()) {
			throw std::runtime_error("websocket control frame is missing string field 'type'");
		}
		if (event_id_it == payload.end() || !event_id_it->is_string()) {
			throw std::runtime_error("websocket control frame is missing string field 'event_id'");
		}
		if (type_it->get<std::string>() != "event") {
			throw std::runtime_error("unsupported websocket control frame type");
		}
		return event_id_it->get<std::string>();
	} catch (const std::runtime_error&) {
		throw;
	} catch (const nlohmann::json::exception& error) {
		throw std::runtime_error(std::string("failed to parse websocket control frame: ") + error.what());
	}
}

void close_lowest_layer(TransportWebSocket& websocket) {
	auto& socket = websocket.next_layer();
	boost::system::error_code ignored;
	socket.cancel(ignored);
	socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
	socket.close(ignored);
}

} // namespace

struct OverlappedFileReader::State {
	struct ReadSlot : OverlappedSlotBase {
		std::uint64_t offset = 0;
		std::size_t requested_length = 0;
	};

	State(
		std::filesystem::path input_path,
		std::uint64_t input_size,
		HANDLE input_handle)
		: path(std::move(input_path)),
		  size(input_size),
		  handle(input_handle) {}

	std::filesystem::path path;
	std::uint64_t offset = 0;
	std::uint64_t size = 0;
	std::uint64_t next_issue_offset = 0;
	std::size_t chunk_size = 0;
	HANDLE handle = INVALID_HANDLE_VALUE;
	std::array<ReadSlot, kOverlappedFileReadQueueDepth> slots;
	std::size_t next_slot_to_issue = 0;
	std::size_t next_slot_to_consume = 0;
	std::size_t in_flight_reads = 0;
	bool prefetch_started = false;
	std::atomic<bool> cancel_requested{false};
	boost::signals2::scoped_connection cancel_connection;
};

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
	std::optional<std::chrono::steady_clock::time_point> fill_started_at;
	std::atomic<bool> cancel_requested{false};
	boost::signals2::scoped_connection cancel_connection;
};

struct FileByteSource::State {
	std::string display_path;
	BufferPool pool;
	std::optional<OverlappedFileReader> reader;
	std::optional<DataChunk> current_chunk;
	std::size_t current_chunk_offset = 0;

	explicit State(const std::filesystem::path& input_path)
		: display_path(path_to_utf8_string(input_path)) {
		UniqueWin32Handle handle(::CreateFileW(
			input_path.c_str(),
			GENERIC_READ,
			FILE_SHARE_READ,
			nullptr,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_OVERLAPPED,
			nullptr));
		if (handle.get() == INVALID_HANDLE_VALUE) {
			throw make_win32_error("failed to open input file: " + display_path);
		}
		const auto input_size = get_file_size(handle.get(), display_path);
		reader.emplace(pool, input_path, input_size, kFileByteSourceChunkSize, handle.release());
		reader->start_prefetch(kFileByteSourceChunkSize * kOverlappedFileReadQueueDepth);
	}
};

OverlappedFileReader::OverlappedFileReader(
	BufferPool& pool,
	const std::filesystem::path& path,
	std::uint64_t size,
	std::size_t buffer_size,
	HANDLE handle)
	: pool_(&pool), state_(std::make_unique<State>(path, size, handle)) {
	state_->chunk_size = std::max<std::size_t>(1, buffer_size);
	initialize_open_state();
}

OverlappedFileReader::~OverlappedFileReader() {
	close();
}

OverlappedFileReader::OverlappedFileReader(OverlappedFileReader&& other)
	: pool_(other.pool_), state_(std::move(other.state_)) {
	other.pool_ = nullptr;
}

OverlappedFileReader& OverlappedFileReader::operator=(OverlappedFileReader&& other) {
	if (this != &other) {
		close();
		pool_ = other.pool_;
		state_ = std::move(other.state_);
		other.pool_ = nullptr;
	}
	return *this;
}

void OverlappedFileReader::listenCancelSignal(CancelEvent& event) {
	if (!state_) {
		return;
	}
	auto* state = state_.get();
	state_->cancel_connection = event.connect([state] {
		state->cancel_requested.store(true, std::memory_order_release);
		if (state->handle != INVALID_HANDLE_VALUE) {
			::CancelIoEx(state->handle, nullptr);
		}
	});
}

void OverlappedFileReader::close() {
	if (!state_) {
		return;
	}

	if (state_->handle != INVALID_HANDLE_VALUE) {
		for (auto& slot : state_->slots) {
			if (slot.in_flight) {
				::CancelIoEx(state_->handle, &slot.overlapped);
				DWORD ignored = 0;
				::GetOverlappedResult(state_->handle, &slot.overlapped, &ignored, TRUE);
				slot.in_flight = false;
			}
			slot.buffer.reset();
			slot.requested_length = 0;
			slot.offset = 0;
		}
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}

	state_.reset();
}

std::string OverlappedFileReader::path_for_error() const {
	return state_ == nullptr ? std::string() : path_to_utf8_string(state_->path);
}

bool OverlappedFileReader::is_cancelled() const {
	return state_ != nullptr && state_->cancel_requested.load(std::memory_order_acquire);
}

void OverlappedFileReader::cancel_pending_work() {
	if (!state_) {
		return;
	}
	state_->cancel_requested.store(true, std::memory_order_release);
	if (state_->handle != INVALID_HANDLE_VALUE) {
		::CancelIoEx(state_->handle, nullptr);
	}
}

bool OverlappedFileReader::issue_next_read() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("file reader cancelled");
	}
	if (state_->next_issue_offset >= state_->size || state_->in_flight_reads >= state_->slots.size()) {
		return false;
	}

	const auto request_length = static_cast<std::size_t>(std::min<std::uint64_t>(
		state_->chunk_size,
		state_->size - state_->next_issue_offset));

	auto slot_index = state_->slots.size();
	for (std::size_t probe = 0; probe < state_->slots.size(); ++probe) {
		const auto index = (state_->next_slot_to_issue + probe) % state_->slots.size();
		const auto& candidate = state_->slots[index];
		if (!candidate.in_flight && !candidate.buffer) {
			slot_index = index;
			break;
		}
	}
	if (slot_index == state_->slots.size()) {
		return false;
	}

	auto& slot = state_->slots[slot_index];
	slot.offset = state_->next_issue_offset;
	slot.requested_length = request_length;

	try {
		slot.buffer = pool_->acquire(slot.requested_length);
	} catch (...) {
		slot.requested_length = 0;
		slot.offset = 0;
		throw;
	}

	slot.overlapped = {};
	slot.overlapped.Offset = static_cast<DWORD>(slot.offset & 0xffffffffull);
	slot.overlapped.OffsetHigh = static_cast<DWORD>((slot.offset >> 32) & 0xffffffffull);
	slot.overlapped.hEvent = slot.event_handle;
	::ResetEvent(slot.event_handle);

	DWORD bytes_read = 0;
	const auto ok = ::ReadFile(
		state_->handle,
		slot.buffer.get(),
		static_cast<DWORD>(slot.requested_length),
		&bytes_read,
		&slot.overlapped);

	if (!ok) {
		const auto error = ::GetLastError();
		if (error != ERROR_IO_PENDING) {
			slot.buffer.reset();
			slot.requested_length = 0;
			slot.offset = 0;
			throw std::runtime_error("failed to read input file: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
		}
	}

	slot.in_flight = true;
	state_->next_slot_to_issue = (slot_index + 1) % state_->slots.size();
	state_->next_issue_offset += slot.requested_length;
	++state_->in_flight_reads;
	return true;
}

void OverlappedFileReader::prime_prefetch_window(std::size_t max_bytes) {
	if (!state_ || state_->size == 0 || max_bytes == 0) {
		return;
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("file reader cancelled");
	}

	std::size_t issued_bytes = 0;
	while (state_->in_flight_reads < state_->slots.size()) {
		const auto before = state_->next_issue_offset;
		if (!issue_next_read()) {
			break;
		}
		issued_bytes += static_cast<std::size_t>(state_->next_issue_offset - before);
		if (issued_bytes >= max_bytes) {
			break;
		}
	}
}

void OverlappedFileReader::initialize_open_state() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("file reader cancelled");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is missing an input handle: " + path_for_error());
	}
	state_->offset = 0;
	state_->next_issue_offset = 0;
	state_->next_slot_to_issue = 0;
	state_->next_slot_to_consume = 0;
	state_->in_flight_reads = 0;
	state_->prefetch_started = false;
}

void OverlappedFileReader::start_prefetch(std::size_t max_bytes) {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("file reader cancelled");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}
	if (state_->prefetch_started) {
		return;
	}
	state_->prefetch_started = true;
	prime_prefetch_window(max_bytes);
}

DataChunk OverlappedFileReader::read_next_chunk() {
	if (!state_) {
		throw std::runtime_error("file reader is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_acquire)) {
		throw CancelledError("file reader cancelled");
	}
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("file reader is not open: " + path_for_error());
	}

	const auto current_offset = state_->offset;
	if (current_offset >= state_->size) {
		return DataChunk{pool_->acquire(0), 0, current_offset, true};
	}

	if (state_->in_flight_reads == 0) {
		issue_next_read();
	}
	if (state_->in_flight_reads == 0) {
		return DataChunk{pool_->acquire(0), 0, current_offset, true};
	}

	auto& slot = state_->slots[state_->next_slot_to_consume];
	if (!slot.in_flight) {
		throw std::runtime_error("file reader consume slot is not in flight");
	}
	DWORD bytes_read = 0;
	if (!::GetOverlappedResult(state_->handle, &slot.overlapped, &bytes_read, TRUE)) {
		const auto error = ::GetLastError();
		slot.in_flight = false;
		slot.buffer.reset();
		slot.requested_length = 0;
		slot.offset = 0;
		if (is_cancelled_win32_error(state_->cancel_requested.load(std::memory_order_acquire), error)) {
			throw CancelledError("file reader cancelled");
		}
		throw std::runtime_error("failed to read input file: " + path_for_error() + ": " + std::system_category().message(static_cast<int>(error)));
	}
	slot.in_flight = false;
	state_->next_slot_to_consume = (state_->next_slot_to_consume + 1) % state_->slots.size();
	--state_->in_flight_reads;

	if (bytes_read != slot.requested_length) {
		slot.buffer.reset();
		slot.requested_length = 0;
		slot.offset = 0;
		throw std::runtime_error("unexpected short read from input file: " + path_for_error());
	}

	state_->offset = slot.offset + bytes_read;
	auto data = std::move(slot.buffer);
	const auto read_offset = slot.offset;
	slot.requested_length = 0;
	slot.offset = 0;

	if (state_->offset < state_->size) {
		issue_next_read();
	}

	return DataChunk{std::move(data), bytes_read, read_offset, state_->offset >= state_->size};
}

std::uint64_t OverlappedFileReader::offset() const {
	return state_ == nullptr ? 0 : state_->offset;
}

bool OverlappedFileReader::eof() const {
	return state_ == nullptr || state_->offset >= state_->size;
}

bool OverlappedFileReader::is_open() const {
	return state_ != nullptr && state_->handle != INVALID_HANDLE_VALUE;
}

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
		const auto now = std::chrono::steady_clock::now();
		if (active_slot.size != 0
			&& state_->fill_started_at.has_value()
			&& now - *state_->fill_started_at >= kSocketIoMaxBufferedSendDelay) {
			submit_active_write();
			continue;
		}
		if (active_slot.size == state_->write_buffer_capacity) {
			submit_active_write();
		}

		const auto available = state_->write_buffer_capacity - active_slot.size;
		const auto chunk = std::min<std::size_t>(available, bytes.size());
		const auto was_empty = active_slot.size == 0;
		std::memcpy(active_slot.buffer.get() + active_slot.size, bytes.data(), chunk);
		active_slot.size += chunk;
		bytes = bytes.subspan(chunk);
		if (was_empty) {
			state_->fill_started_at = now;
		}

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
	state_->write_slots[state_->active_slot_index].offset = 0;
	state_->fill_started_at.reset();
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

FileByteSource::FileByteSource(const std::filesystem::path& input_path) : state_(std::make_unique<State>(input_path)) {}

FileByteSource::~FileByteSource() = default;

std::size_t FileByteSource::read(uint8_t* buffer, std::size_t length) {
	if (!state_ || !state_->reader.has_value() || !state_->reader->is_open()) {
		throw std::runtime_error("input file is closed");
	}
	if (length == 0) {
		return 0;
	}

	std::size_t copied_total = 0;
	while (copied_total < length) {
		if (!state_->current_chunk.has_value() || state_->current_chunk_offset >= state_->current_chunk->length) {
			state_->current_chunk = state_->reader->read_next_chunk();
			state_->current_chunk_offset = 0;
			if (state_->current_chunk->length == 0) {
				break;
			}
		}

		const auto available = state_->current_chunk->length - state_->current_chunk_offset;
		const auto chunk = std::min<std::size_t>(available, length - copied_total);
		std::memcpy(
			buffer + copied_total,
			state_->current_chunk->data.get() + state_->current_chunk_offset,
			chunk);
		state_->current_chunk_offset += chunk;
		copied_total += chunk;

		if (state_->current_chunk_offset >= state_->current_chunk->length) {
			state_->current_chunk.reset();
			state_->current_chunk_offset = 0;
		}
	}

	return copied_total;
}

struct SocketByteSink::State {
	explicit State(TransportWebSocket ws, bool enable_keepalive)
		: websocket(std::move(ws)), keepalive_enabled(enable_keepalive) {
		data_buffer.resize(kSocketIoBufferSize);
		control_writer = std::make_unique<detail2::WriteBuffer>(
			kControlBufferCapacity,
			kSocketIoMaxBufferedSendDelay,
			[this](std::span<const uint8_t> data) {
				do_send_control_frame(data);
			});
		touch_activity();
		if (keepalive_enabled) {
			keepalive_thread = std::jthread([this](std::stop_token stop_token) {
				run_keepalive(stop_token);
			});
		}
	}

	~State() {
		stop();
	}

	void send_transport_begin(bool file_comparison = false) {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		if (transport_active) {
			throw std::runtime_error("transport has already begun");
		}
		std::scoped_lock send_lock(send_mutex);
		flush_data_buffer();
		control_writer->flush();

		nlohmann::json payload = {
			{"type", "event"},
			{"event_id", kTransportBeginEvent},
		};
		if (file_comparison) {
			payload["file_comparison"] = true;
		}
		do_send_control_frame(make_string_span(payload.dump()));
		transport_active = true;
	}

	void write(std::span<const uint8_t> bytes) {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		if (!transport_active) {
			throw std::runtime_error("transport has not begun");
		}

		std::scoped_lock send_lock(send_mutex);
		control_writer->flush();
		std::size_t offset = 0;
		while (offset < bytes.size()) {
			const auto available = data_buffer.size() - data_buffered_size;
			if (available == 0) {
				flush_data_buffer();
				continue;
			}

			const auto was_empty = data_buffered_size == 0;
			const auto now = std::chrono::steady_clock::now();
			const auto chunk = std::min<std::size_t>(available, bytes.size() - offset);
			std::memcpy(data_buffer.data() + data_buffered_size, bytes.data() + offset, chunk);
			data_buffered_size += chunk;
			offset += chunk;
			if (was_empty) {
				data_fill_started_at = now;
			}
			if (data_buffered_size == data_buffer.size()
				|| (data_fill_started_at.has_value() && now - *data_fill_started_at >= kSocketIoMaxBufferedSendDelay)) {
				flush_data_buffer();
			}
		}
	}

	void close() {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		std::scoped_lock send_lock(send_mutex);
		flush_data_buffer();
		control_writer->flush();
	}

	void send_transport_end() {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		if (!transport_active) {
			throw std::runtime_error("transport has not begun");
		}
		std::scoped_lock send_lock(send_mutex);
		flush_data_buffer();
		control_writer->flush();
		do_send_control_frame(make_string_span(make_transport_event_payload(kTransportEndEvent)));
		transport_active = false;
	}

	void send_control_message(std::string_view json_payload) {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		std::scoped_lock send_lock(send_mutex);
		flush_data_buffer();
		std::vector<uint8_t> record;
		record.reserve(kControlMessageLengthSize + json_payload.size());
		append_control_message_record(record, json_payload);
		if (record.size() > kControlBufferCapacity) {
			control_writer->flush();
			do_send_control_frame(record);
			return;
		}
		if (control_writer->buffered_size() + record.size() > kControlBufferCapacity) {
			control_writer->flush();
		}
		control_writer->write(record);
	}

	void flush_control_buffer() {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		std::scoped_lock send_lock(send_mutex);
		control_writer->flush();
	}

	void check_connection() const {
		rethrow_async_error();
	}

	void close_socket() {
		request_keepalive_stop();
		close_lowest_layer(websocket);
		socket_closed.store(true, std::memory_order_relaxed);
	}

	void stop() {
		if (stop_completed.exchange(true, std::memory_order_relaxed)) {
			return;
		}
		request_keepalive_stop();
		close_lowest_layer(websocket);
		socket_closed.store(true, std::memory_order_relaxed);
	}

	void cancel_pending_work() {
		if (cancel_requested.exchange(true, std::memory_order_relaxed)) {
			return;
		}
		request_keepalive_stop();
		close_lowest_layer(websocket);
		socket_closed.store(true, std::memory_order_relaxed);
	}

	// 发送数据通道帧：header(kData) + payload
	void flush_data_buffer() {
		if (data_buffered_size == 0) {
			return;
		}
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}
		do_send_data_frame(std::span<const uint8_t>(data_buffer.data(), data_buffered_size));
		data_buffered_size = 0;
		data_fill_started_at.reset();
	}

	// 发送数据通道帧（带 header）
	void do_send_data_frame(std::span<const uint8_t> payload) {
		// 构建: [8 字节 header] + [payload]
		std::vector<uint8_t> framed;
		framed.reserve(detail2::TransportFrameHeader::kSerializedSize + payload.size());
		detail2::TransportFrameHeader::write_header(
			framed,
			detail2::TransportFrameHeader::kData,
			payload.size());
		framed.insert(framed.end(), payload.begin(), payload.end());

		boost::system::error_code error;
		{
			std::scoped_lock lock(operation_mutex);
			websocket.binary(true);
			websocket.write(boost::asio::buffer(framed.data(), framed.size()), error);
		}
		if (error) {
			if (is_cancelled_socket_error(cancel_requested.load(std::memory_order_relaxed), error)) {
				throw CancelledError("socket send cancelled");
			}
			throw make_socket_error("websocket data frame write failed", error);
		}
		touch_activity();
	}

	// 发送控制通道帧：header(kControl) + JSON payload
	void do_send_control_frame(std::span<const uint8_t> payload) {
		rethrow_async_error();
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket send cancelled");
		}

		std::vector<uint8_t> framed;
		framed.reserve(detail2::TransportFrameHeader::kSerializedSize + payload.size());
		detail2::TransportFrameHeader::write_header(
			framed,
			detail2::TransportFrameHeader::kControl,
			payload.size());
		framed.insert(framed.end(), payload.begin(), payload.end());

		boost::system::error_code error;
		{
			std::scoped_lock lock(operation_mutex);
			websocket.binary(true);
			websocket.write(boost::asio::buffer(framed.data(), framed.size()), error);
		}
		if (error) {
			if (is_cancelled_socket_error(cancel_requested.load(std::memory_order_relaxed), error)) {
				throw CancelledError("socket send cancelled");
			}
			throw make_socket_error("websocket control frame write failed", error);
		}
		touch_activity();
	}

	static std::span<const uint8_t> make_string_span(std::string_view sv) {
		return std::span<const uint8_t>(
			reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
	}

	void run_keepalive(std::stop_token stop_token) {
		std::unique_lock lock(keepalive_mutex);
		while (!stop_token.stop_requested() && !keepalive_stop_requested) {
			keepalive_cv.wait_for(lock, kSocketKeepalivePollInterval, [&] {
				return stop_token.stop_requested() || keepalive_stop_requested;
			});
			if (stop_token.stop_requested() || keepalive_stop_requested) {
				break;
			}
			if (cancel_requested.load(std::memory_order_relaxed) || socket_closed.load(std::memory_order_relaxed)) {
				break;
			}
			const auto idle_for = std::chrono::milliseconds(
				steady_clock_millis() - last_activity_millis.load(std::memory_order_relaxed));
			if (idle_for < kSocketKeepaliveIdleInterval) {
				continue;
			}
			lock.unlock();
			boost::system::error_code error;
			{
				std::scoped_lock op_lock(operation_mutex);
				if (!cancel_requested.load(std::memory_order_relaxed) && !socket_closed.load(std::memory_order_relaxed)) {
					websocket.ping({}, error);
				}
			}
			if (error) {
				store_async_error(std::make_exception_ptr(make_socket_error("websocket ping failed", error)));
				close_lowest_layer(websocket);
				socket_closed.store(true, std::memory_order_relaxed);
				break;
			}
			touch_activity();
			lock.lock();
		}
	}

	void request_keepalive_stop() {
		{
			std::lock_guard lock(keepalive_mutex);
			keepalive_stop_requested = true;
		}
		keepalive_cv.notify_all();
		if (keepalive_thread.joinable()) {
			keepalive_thread.request_stop();
			keepalive_thread.join();
		}
	}

	void touch_activity() {
		last_activity_millis.store(steady_clock_millis(), std::memory_order_relaxed);
	}

	void store_async_error(std::exception_ptr error) {
		std::lock_guard lock(async_error_mutex);
		if (!async_error) {
			async_error = std::move(error);
		}
	}

	void rethrow_async_error() const {
		std::lock_guard lock(async_error_mutex);
		if (async_error) {
			std::rethrow_exception(async_error);
		}
	}

	TransportWebSocket websocket;
	std::vector<uint8_t> data_buffer;
	std::size_t data_buffered_size = 0;
	std::optional<std::chrono::steady_clock::time_point> data_fill_started_at;
	bool transport_active = false;
	const bool keepalive_enabled = false;
	std::unique_ptr<detail2::WriteBuffer> control_writer;
	std::deque<std::string> pending_control_responses;
	std::jthread keepalive_thread;
	mutable std::mutex async_error_mutex;
	std::exception_ptr async_error;
	std::mutex send_mutex;
	std::mutex operation_mutex;
	std::mutex keepalive_mutex;
	std::condition_variable keepalive_cv;
	bool keepalive_stop_requested = false;
	std::atomic<std::int64_t> last_activity_millis{0};
	std::atomic<bool> socket_closed{false};
	std::atomic<bool> stop_completed{false};
	std::atomic<bool> cancel_requested{false};
	boost::signals2::scoped_connection cancel_connection;
};

SocketByteSink::SocketByteSink(TransportWebSocket websocket, bool enable_keepalive_ping)
	: state_(std::make_unique<State>(std::move(websocket), enable_keepalive_ping)) {}

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

void SocketByteSink::send_transport_begin(bool file_comparison) {
	state_->send_transport_begin(file_comparison);
}

void SocketByteSink::send_transport_end() {
	state_->send_transport_end();
}

void SocketByteSink::write(std::span<const uint8_t> bytes) {
	state_->write(bytes);
}

void SocketByteSink::close() {
	state_->close();
}

void SocketByteSink::send_control_message(std::string_view json_payload) {
	state_->send_control_message(json_payload);
}

void SocketByteSink::flush_control_buffer() {
	state_->flush_control_buffer();
}

std::string SocketByteSink::await_control_response() {
	if (!state_) {
		throw std::runtime_error("socket sink is closed");
	}
	if (!state_->pending_control_responses.empty()) {
		auto response = std::move(state_->pending_control_responses.front());
		state_->pending_control_responses.pop_front();
		return response;
	}

	// 复用 SocketByteSource 的帧读取逻辑来读一条控制帧
	detail2::TransportFrameHeader header;
	std::vector<uint8_t> payload;

	boost::beast::flat_buffer input_buffer;
	input_buffer.reserve(kSocketIoBufferSize);
	boost::system::error_code error;
	{
		std::scoped_lock lock(state_->operation_mutex);
		state_->websocket.read(input_buffer, error);
	}
	if (error == boost::beast::websocket::error::closed) {
		throw std::runtime_error("websocket closed while awaiting control response");
	}
	if (error) {
		if (is_cancelled_socket_error(state_->cancel_requested.load(std::memory_order_relaxed), error)) {
			throw CancelledError("socket send cancelled");
		}
		throw make_socket_error("websocket control response read failed", error);
	}

	const auto total_size = input_buffer.size();
	if (total_size < detail2::TransportFrameHeader::kSerializedSize) {
		throw std::runtime_error("received frame too small for transport header");
	}

	std::vector<uint8_t> raw_frame(total_size);
	boost::asio::buffer_copy(boost::asio::buffer(raw_frame), input_buffer.data());

	std::array<uint8_t, detail2::TransportFrameHeader::kSerializedSize> header_buf{};
	std::memcpy(header_buf.data(), raw_frame.data(), header_buf.size());
	header = detail2::TransportFrameHeader::deserialize_from(
		std::span<const uint8_t, detail2::TransportFrameHeader::kSerializedSize>(header_buf));

	if (header.channel != detail2::TransportFrameHeader::kControl) {
		throw std::runtime_error("expected control frame but received data frame");
	}

	payload.assign(
		raw_frame.begin() + detail2::TransportFrameHeader::kSerializedSize,
		raw_frame.end());
	auto messages = decode_control_messages(payload);
	if (messages.empty()) {
		return {};
	}
	for (std::size_t i = 1; i < messages.size(); ++i) {
		state_->pending_control_responses.push_back(std::move(messages[i]));
	}
	return std::move(messages.front());
}

void SocketByteSink::check_connection() {
	state_->check_connection();
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
	explicit State(TransportWebSocket ws)
		: websocket(std::move(ws)) {}

	~State() {
		stop();
	}

	bool await_transport_begin() {
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket receive cancelled");
		}
		transport_finished = false;
		file_comparison_enabled = false;
		active_message.clear();
		active_offset = 0;

		for (;;) {
			detail2::TransportFrameHeader header;
			std::vector<uint8_t> payload;
			if (!read_next_frame(header, payload)) {
				return false;
			}
			if (header.channel != detail2::TransportFrameHeader::kControl) {
				throw std::runtime_error("received data frame before transport_begin");
			}
			auto payload_text = std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size());
			auto event_id = parse_transport_event_payload(payload_text);
			if (event_id == kTransportBeginEvent) {
				// 解析可选的 file_comparison 标记
				try {
					auto json = nlohmann::json::parse(payload_text);
					if (json.is_object()) {
						auto fc_it = json.find("file_comparison");
						if (fc_it != json.end() && fc_it->is_boolean() && fc_it->get<bool>()) {
							file_comparison_enabled = true;
						}
					}
				} catch (...) {
					// JSON 解析失败忽略
				}
				transport_active = true;
				return true;
			}
			if (event_id == kTransportEndEvent) {
				throw std::runtime_error("received transport_end before transport_begin");
			}
			// 其他控制消息在此阶段忽略（兼容未来扩展）
		}
	}

	bool await_control_message(std::string& json_out) {
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket receive cancelled");
		}

		for (;;) {
			if (!pending_control_messages.empty()) {
				json_out = std::move(pending_control_messages.front());
				pending_control_messages.pop_front();
				return true;
			}

			detail2::TransportFrameHeader header;
			std::vector<uint8_t> payload;
			if (!read_next_frame(header, payload)) {
				return false;
			}
			if (header.channel != detail2::TransportFrameHeader::kControl) {
				throw std::runtime_error("received unexpected data frame while awaiting control message");
			}

			auto messages = decode_control_messages(payload);
			if (messages.empty()) {
				continue;
			}
			if (messages.size() > 1) {
				for (std::size_t i = 1; i < messages.size(); ++i) {
					pending_control_messages.push_back(std::move(messages[i]));
				}
			}

			auto payload_text = std::string_view(messages.front().data(), messages.front().size());

			// 先检查 type 字段，区分 event 类控制消息和普通控制消息
			try {
				auto json = nlohmann::json::parse(payload_text);
				if (!json.is_object()) {
					throw std::runtime_error("control frame must be a JSON object");
				}
				const auto type_it = json.find("type");
				std::string msg_type;
				if (type_it != json.end() && type_it->is_string()) {
					msg_type = type_it->get<std::string>();
				}

				if (msg_type == "event") {
					// 标准事件消息
					auto event_id = parse_transport_event_payload(payload_text);
					if (event_id == kTransportEndEvent) {
						json_out = std::move(messages.front());
						return true;
					}
					if (event_id == kTransportBeginEvent) {
						throw std::runtime_error("received unexpected transport_begin");
					}
					// 其他事件消息也返回原始 JSON
					json_out = std::move(messages.front());
					return true;
				}

				// 非 event 类型的控制消息（如 file_info_batch, file_info_diff）
				json_out = std::move(messages.front());
				return true;
			} catch (const std::runtime_error&) {
				throw;
			} catch (const nlohmann::json::exception& error) {
				throw std::runtime_error(std::string("failed to parse control frame: ") + error.what());
			}
		}
	}

	std::size_t read(uint8_t* output, std::size_t length) {
		if (length == 0) {
			return 0;
		}
		if (cancel_requested.load(std::memory_order_relaxed)) {
			throw CancelledError("socket receive cancelled");
		}
		if (!transport_active) {
			if (transport_finished) {
				return 0;
			}
			throw std::runtime_error("transport has not begun");
		}

		std::size_t copied_total = 0;
		while (copied_total < length) {
			while (active_offset < active_message.size() && copied_total < length) {
				const auto available = active_message.size() - active_offset;
				const auto chunk = std::min<std::size_t>(available, length - copied_total);
				std::memcpy(output + copied_total, active_message.data() + active_offset, chunk);
				active_offset += chunk;
				copied_total += chunk;
			}
			if (active_offset < active_message.size()) {
				return copied_total;
			}
			active_message.clear();
			active_offset = 0;

			detail2::TransportFrameHeader header;
			std::vector<uint8_t> payload;
			if (!read_next_frame(header, payload)) {
				if (copied_total != 0) {
					return copied_total;
				}
				throw std::runtime_error("websocket connection closed before transport_end");
			}
			if (header.channel == detail2::TransportFrameHeader::kControl) {
				auto messages = decode_control_messages(payload);
				bool saw_transport_end = false;
				for (auto& message : messages) {
					auto payload_text = std::string_view(message.data(), message.size());
					try {
						auto json = nlohmann::json::parse(payload_text);
						if (json.is_object()) {
							const auto type_it = json.find("type");
							if (type_it != json.end() && type_it->is_string() && type_it->get<std::string>() == "event") {
								const auto event_id = parse_transport_event_payload(payload_text);
								if (event_id == kTransportEndEvent) {
									saw_transport_end = true;
									continue;
								}
								if (event_id == kTransportBeginEvent) {
									throw std::runtime_error("received unexpected transport_begin while receiving data");
								}
								throw std::runtime_error("unexpected control event while receiving data: " + event_id);
							}
						}
					} catch (const nlohmann::json::exception& error) {
						throw std::runtime_error(std::string("failed to parse control frame while receiving data: ") + error.what());
					}

					if (!data_control_handler) {
						throw std::runtime_error("unexpected control message while receiving data");
					}
					data_control_handler(payload_text);
				}

				if (saw_transport_end) {
					if (messages.size() > 1) {
						throw std::runtime_error("transport_end control frame contained additional messages");
					}
					transport_active = false;
					transport_finished = true;
					return copied_total;
				}
				continue;
			}
			// 数据帧：直接作为 active_message 供后续 read 消费
			active_message = std::move(payload);
		}
		return copied_total;
	}

	void close_socket() {
		close_lowest_layer(websocket);
		socket_closed.store(true, std::memory_order_relaxed);
	}

	void stop() {
		if (stop_completed.exchange(true, std::memory_order_relaxed)) {
			return;
		}
		close_socket();
	}

	void cancel_pending_work() {
		if (cancel_requested.exchange(true, std::memory_order_relaxed)) {
			return;
		}
		close_socket();
	}

	// 读取一个 WebSocket 二进制消息，解析帧头，返回 channel + payload
	bool read_next_frame(detail2::TransportFrameHeader& header, std::vector<uint8_t>& payload) {
		boost::beast::flat_buffer input_buffer;
		input_buffer.reserve(kSocketIoBufferSize);
		boost::system::error_code error;
		websocket.read(input_buffer, error);
		if (error == boost::beast::websocket::error::closed) {
			socket_closed.store(true, std::memory_order_relaxed);
			return false;
		}
		if (error) {
			if (is_cancelled_socket_error(cancel_requested.load(std::memory_order_relaxed), error)) {
				throw CancelledError("socket receive cancelled");
			}
			throw make_socket_error("websocket read failed", error);
		}

		const auto total_size = input_buffer.size();
		if (total_size < detail2::TransportFrameHeader::kSerializedSize) {
			throw std::runtime_error("received websocket frame too small for transport header");
		}

		// 先全部拷出为原始字节，再切分 header + payload
		std::vector<uint8_t> raw_frame(total_size);
		boost::asio::buffer_copy(boost::asio::buffer(raw_frame), input_buffer.data());

		std::array<uint8_t, detail2::TransportFrameHeader::kSerializedSize> header_buf{};
		std::memcpy(header_buf.data(), raw_frame.data(), header_buf.size());
		header = detail2::TransportFrameHeader::deserialize_from(
			std::span<const uint8_t, detail2::TransportFrameHeader::kSerializedSize>(header_buf));

		const auto payload_size = total_size - detail2::TransportFrameHeader::kSerializedSize;
		if (payload_size != header.payload_size) {
			throw std::runtime_error("transport frame payload size mismatch");
		}

		payload.assign(
			raw_frame.begin() + detail2::TransportFrameHeader::kSerializedSize,
			raw_frame.end());

		return true;
	}

	TransportWebSocket websocket;
	std::vector<uint8_t> active_message;
	std::size_t active_offset = 0;
	std::deque<std::string> pending_control_messages;
	SocketByteSource::ControlMessageHandler data_control_handler;
	bool transport_active = false;
	bool transport_finished = false;
	bool file_comparison_enabled = false;
	std::atomic<bool> socket_closed{false};
	std::atomic<bool> stop_completed{false};
	boost::signals2::scoped_connection cancel_connection;
	std::atomic<bool> cancel_requested{false};
};

SocketByteSource::SocketByteSource(TransportWebSocket websocket) : state_(std::make_unique<State>(std::move(websocket))) {}

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

bool SocketByteSource::await_transport_begin() {
	return state_->await_transport_begin();
}

bool SocketByteSource::transport_has_file_comparison() const {
	return state_ != nullptr && state_->file_comparison_enabled;
}

std::size_t SocketByteSource::read(uint8_t* buffer, std::size_t length) {
	return state_->read(buffer, length);
}

bool SocketByteSource::await_control_message(std::string& json_out) {
	return state_->await_control_message(json_out);
}

void SocketByteSource::set_data_control_message_handler(ControlMessageHandler handler) {
	if (!state_) {
		throw std::runtime_error("socket source is closed");
	}
	state_->data_control_handler = std::move(handler);
}

void SocketByteSource::send_control_message(std::string_view json_payload) {
	if (!state_) {
		throw std::runtime_error("socket source is closed");
	}
	if (state_->cancel_requested.load(std::memory_order_relaxed)) {
		throw CancelledError("socket receive cancelled");
	}

	std::vector<uint8_t> control_payload;
	control_payload.reserve(kControlMessageLengthSize + json_payload.size());
	append_control_message_record(control_payload, json_payload);

	std::vector<uint8_t> framed;
	framed.reserve(detail2::TransportFrameHeader::kSerializedSize + control_payload.size());
	detail2::TransportFrameHeader::write_header(
		framed,
		detail2::TransportFrameHeader::kControl,
		control_payload.size());
	framed.insert(framed.end(), control_payload.begin(), control_payload.end());

	boost::system::error_code error;
	state_->websocket.binary(true);
	state_->websocket.write(boost::asio::buffer(framed.data(), framed.size()), error);
	if (error) {
		if (is_cancelled_socket_error(state_->cancel_requested.load(std::memory_order_relaxed), error)) {
			throw CancelledError("socket receive cancelled");
		}
		throw make_socket_error("websocket control write failed", error);
	}
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
