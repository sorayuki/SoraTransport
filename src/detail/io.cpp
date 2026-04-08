#include "io.hpp"
#include "win32_util.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <cwchar>
#include <deque>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

#include <winioctl.h>

namespace soratransport {

namespace {

constexpr std::size_t kDirectIoBufferSize = 4 * 1024 * 1024;
constexpr std::size_t kBufferedWriteBatchSize = 8 * 1024 * 1024;
constexpr std::size_t kSocketIoBufferSize = 1 * 1024 * 1024;
constexpr std::size_t kSocketIoReceiveBufferSize = kSocketIoBufferSize / 2;

std::runtime_error make_socket_error(std::string_view action, const boost::system::error_code& error) {
	return std::runtime_error(std::string(action) + ": " + error.message());
}

std::wstring make_volume_device_path(const std::filesystem::path& path) {
	const auto absolute_path = std::filesystem::absolute(path);
	const auto root_name = absolute_path.root_name().wstring();
	if (root_name.size() < 2 || root_name[1] != L':') {
		return L"";
	}
	return L"\\\\.\\" + root_name;
}

void write_all(HANDLE handle, std::span<const uint8_t> bytes, const std::string& path) {
	std::size_t written_total = 0;
	while (written_total < bytes.size()) {
		const auto remaining = bytes.size() - written_total;
		const auto chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
		DWORD bytes_written = 0;
		if (!::WriteFile(handle, bytes.data() + written_total, chunk, &bytes_written, nullptr)) {
			throw make_win32_error("failed to write output file: " + path);
		}
		if (bytes_written == 0) {
			throw std::runtime_error("failed to make forward progress while writing output file: " + path);
		}
		written_total += bytes_written;
	}
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

void write_exact_at(HANDLE handle, const uint8_t* buffer, std::size_t length, std::uint64_t offset, const std::string& path) {
	std::size_t written_total = 0;
	while (written_total < length) {
		OVERLAPPED overlapped{};
		overlapped.Offset = static_cast<DWORD>((offset + written_total) & 0xffffffffull);
		overlapped.OffsetHigh = static_cast<DWORD>(((offset + written_total) >> 32) & 0xffffffffull);
		const auto remaining = length - written_total;
		const auto chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
		DWORD bytes_written = 0;
		if (!::WriteFile(handle, buffer + written_total, chunk, &bytes_written, &overlapped)) {
			throw make_win32_error("failed to write output file: " + path);
		}
		if (bytes_written != chunk) {
			throw std::runtime_error("unexpected short write to output file: " + path);
		}
		written_total += bytes_written;
	}
}

std::size_t read_exact_at(HANDLE handle, uint8_t* buffer, std::size_t length, std::uint64_t offset, const std::string& path) {
	std::size_t total_bytes_read = 0;
	while (total_bytes_read < length) {
		OVERLAPPED overlapped{};
		overlapped.Offset = static_cast<DWORD>((offset + total_bytes_read) & 0xffffffffull);
		overlapped.OffsetHigh = static_cast<DWORD>(((offset + total_bytes_read) >> 32) & 0xffffffffull);
		const auto remaining = length - total_bytes_read;
		const auto chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
		DWORD bytes_read = 0;
		if (!::ReadFile(handle, buffer + total_bytes_read, chunk, &bytes_read, &overlapped)) {
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

void set_file_size(HANDLE handle, std::uint64_t size, const std::string& path) {
	LARGE_INTEGER target_size{};
	target_size.QuadPart = static_cast<LONGLONG>(size);
	if (!::SetFilePointerEx(handle, target_size, nullptr, FILE_BEGIN)) {
		throw make_win32_error("failed to seek file: " + path);
	}
	if (!::SetEndOfFile(handle)) {
		throw make_win32_error("failed to resize file: " + path);
	}
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

void resize_file_buffered(const std::filesystem::path& path, std::uint64_t size) {
	const auto handle = ::CreateFileW(
		path.c_str(),
		GENERIC_WRITE,
		0,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
		nullptr);
	if (handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to reopen output file for resize: " + path_to_utf8_string(path));
	}
	try {
		set_file_size(handle, size, path_to_utf8_string(path));
		flush_file_buffers_if_supported(handle, path_to_utf8_string(path), "failed to flush resized output file");
		::CloseHandle(handle);
	} catch (...) {
		::CloseHandle(handle);
		throw;
	}
}

} // namespace

FileIoAlignmentInfo query_file_io_alignment(const std::filesystem::path& path) {
	FileIoAlignmentInfo info;
	const auto absolute_path = std::filesystem::absolute(path);
	std::wstring volume_path(MAX_PATH, L'\0');
	if (!::GetVolumePathNameW(absolute_path.c_str(), volume_path.data(), static_cast<DWORD>(volume_path.size()))) {
		return info;
	}
	volume_path.resize(std::wcslen(volume_path.c_str()));

	DWORD sectors_per_cluster = 0;
	DWORD bytes_per_sector = 0;
	DWORD number_of_free_clusters = 0;
	DWORD total_number_of_clusters = 0;
	if (::GetDiskFreeSpaceW(
			volume_path.c_str(),
			&sectors_per_cluster,
			&bytes_per_sector,
			&number_of_free_clusters,
			&total_number_of_clusters)) {
		info.logical_sector_size = std::max<std::size_t>(kFileIoAlignment, bytes_per_sector);
		info.physical_sector_size = info.logical_sector_size;
		info.required_alignment = info.logical_sector_size;
	}

	const auto volume_device = make_volume_device_path(absolute_path);
	if (!volume_device.empty()) {
		const auto handle = ::CreateFileW(
			volume_device.c_str(),
			0,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			OPEN_EXISTING,
			0,
			nullptr);
		if (handle != INVALID_HANDLE_VALUE) {
			STORAGE_PROPERTY_QUERY query{};
			query.PropertyId = StorageAccessAlignmentProperty;
			query.QueryType = PropertyStandardQuery;
			STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR descriptor{};
			DWORD bytes_returned = 0;
			if (::DeviceIoControl(
					handle,
					IOCTL_STORAGE_QUERY_PROPERTY,
					&query,
					static_cast<DWORD>(sizeof(query)),
					&descriptor,
					static_cast<DWORD>(sizeof(descriptor)),
					&bytes_returned,
					nullptr) && descriptor.BytesPerPhysicalSector > 0) {
				info.physical_sector_size = std::max<std::size_t>(info.logical_sector_size, descriptor.BytesPerPhysicalSector);
				info.required_alignment = std::max(info.required_alignment, info.physical_sector_size);
			}
			::CloseHandle(handle);
		}
	}

	return info;
}

struct FileByteSink::State {
	struct WriteSlot : OverlappedSlotBase {
		std::size_t size = 0;
		std::uint64_t offset = 0;
	};

	HANDLE handle = INVALID_HANDLE_VALUE;
	std::filesystem::path path;
	std::string display_path;
	bool closed = false;
	FileIoMode mode = FileIoMode::Buffered;
	std::size_t io_alignment = kFileIoAlignment;
	std::uint64_t logical_size = 0;
	std::size_t write_buffer_capacity = 0;
	std::size_t max_in_flight_write_ops = 1;
	std::uint64_t physical_size = 0;
	std::vector<WriteSlot> write_slots;
	std::deque<std::size_t> available_slots;
	std::deque<std::size_t> in_flight_slots;
	std::size_t active_slot_index = 0;
};

struct FileByteSource::State {
	HANDLE handle = INVALID_HANDLE_VALUE;
	std::filesystem::path path;
	std::string display_path;
	FileIoMode mode = FileIoMode::Buffered;
	std::size_t io_alignment = kFileIoAlignment;
	std::uint64_t logical_offset = 0;
	std::uint64_t size = 0;
	std::shared_ptr<uint8_t> aligned_buffer;
	std::size_t aligned_buffer_capacity = 0;
	std::size_t aligned_buffer_size = 0;
	std::size_t aligned_buffer_offset = 0;
};

FileByteSink::FileByteSink(const std::filesystem::path& output_path, std::size_t max_in_flight_write_ops) : state_(std::make_unique<State>()) {
	state_->path = output_path;
	state_->display_path = path_to_utf8_string(output_path);
	state_->mode = FileIoMode::Buffered;
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

void FileByteSink::flush_pending_writes() {
	submit_active_write(false);
}

void FileByteSink::write(std::span<const uint8_t> bytes) {
	if (!state_ || state_->closed || state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("output file is closed");
	}
	state_->logical_size += bytes.size();
	while (!bytes.empty()) {
		auto& active_slot = state_->write_slots[state_->active_slot_index];
		if (active_slot.size == state_->write_buffer_capacity) {
			submit_active_write(false);
		}

		const auto available = state_->write_buffer_capacity - active_slot.size;
		const auto chunk = std::min<std::size_t>(available, bytes.size());
		std::memcpy(active_slot.buffer.get() + active_slot.size, bytes.data(), chunk);
		active_slot.size += chunk;
		bytes = bytes.subspan(chunk);

		if (active_slot.size == state_->write_buffer_capacity) {
			submit_active_write(false);
		}
	}
}

void FileByteSink::submit_active_write(bool finalize) {
	if (!state_) {
		return;
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
	submit_active_write(true);
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

FileByteSource::FileByteSource(const std::filesystem::path& input_path, FileIoMode mode) : state_(std::make_unique<State>()) {
	state_->path = input_path;
	state_->display_path = path_to_utf8_string(input_path);
	state_->mode = mode;
	if (mode == FileIoMode::Direct) {
		state_->io_alignment = query_file_io_alignment(input_path).required_alignment;
	}
	const auto flags = mode == FileIoMode::Direct
		? FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_NO_BUFFERING
		: FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN;
	state_->handle = ::CreateFileW(
		input_path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		flags,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open input file: " + state_->display_path);
	}
	LARGE_INTEGER file_size{};
	if (!::GetFileSizeEx(state_->handle, &file_size)) {
		const auto error = ::GetLastError();
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to query input file size: " + state_->display_path, error);
	}
	state_->size = static_cast<std::uint64_t>(file_size.QuadPart);
	if (mode == FileIoMode::Direct) {
		state_->aligned_buffer_capacity = static_cast<std::size_t>(round_up(kDirectIoBufferSize, state_->io_alignment));
		state_->aligned_buffer = make_aligned_buffer(state_->aligned_buffer_capacity, state_->io_alignment);
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
	if (state_->mode == FileIoMode::Buffered) {
		return read_some(state_->handle, buffer, length, state_->display_path);
	}

	std::size_t copied_total = 0;
	while (copied_total < length) {
		if (state_->aligned_buffer_offset < state_->aligned_buffer_size) {
			const auto available = state_->aligned_buffer_size - state_->aligned_buffer_offset;
			const auto chunk = std::min<std::size_t>(available, length - copied_total);
			std::memcpy(buffer + copied_total, state_->aligned_buffer.get() + state_->aligned_buffer_offset, chunk);
			state_->aligned_buffer_offset += chunk;
			state_->logical_offset += chunk;
			copied_total += chunk;
			continue;
		}

		if (state_->logical_offset >= state_->size) {
			break;
		}

		const auto remaining = state_->size - state_->logical_offset;
		std::size_t request_size = 0;
		if (remaining >= state_->io_alignment) {
			request_size = static_cast<std::size_t>(std::min<std::uint64_t>(round_down(remaining, state_->io_alignment), state_->aligned_buffer_capacity));
			if (request_size == 0) {
				request_size = state_->io_alignment;
			}
		} else {
			request_size = state_->io_alignment;
		}

		const auto bytes_read = read_exact_at(state_->handle, state_->aligned_buffer.get(), request_size, state_->logical_offset, state_->display_path);
		if (bytes_read == 0) {
			break;
		}
		state_->aligned_buffer_size = bytes_read;
		state_->aligned_buffer_offset = 0;
	}

	return copied_total;
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

			const auto chunk = std::min<std::size_t>(available, bytes.size() - offset);
			std::memcpy(buffers[fill_index].data() + fill_size, bytes.data() + offset, chunk);
			fill_size += chunk;
			offset += chunk;
			if (fill_size == kSocketIoBufferSize) {
				submit_fill_buffer();
			}
		}
	}

	void close() {
		if (closed) {
			rethrow_async_error();
			return;
		}
		rethrow_async_error();
		submit_fill_buffer();
		wait_for_pending_send();
		closed = true;
		shutdown_socket();
		rethrow_async_error();
	}

	void stop() {
		if (stopped) {
			return;
		}
		stopped = true;
		if (send_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
			shutdown_socket();
			wait_for_pending_send();
		} else {
			shutdown_socket();
		}
	}

	void submit_fill_buffer() {
		if (sizes[fill_index] == 0) {
			return;
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
			if (!async_error && !(stopped && send_error == boost::asio::error::operation_aborted)) {
				async_error = std::make_exception_ptr(make_socket_error("socket write failed", send_error));
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

	boost::asio::ip::tcp::socket socket;
	boost::asio::io_context& io_context;
	std::array<std::vector<uint8_t>, 2> buffers;
	std::array<std::size_t, 2> sizes{};
	std::size_t fill_index = 0;
	std::optional<std::size_t> completed_index;
	bool closed = false;
	bool stopped = false;
	bool send_in_flight = false;
	bool send_completed = false;
	std::size_t send_bytes_transferred = 0;
	boost::system::error_code send_error;
	std::exception_ptr async_error;
	bool socket_shutdown = false;
};

SocketByteSink::SocketByteSink(boost::asio::ip::tcp::socket socket) : state_(std::make_unique<State>(std::move(socket))) {}

SocketByteSink::~SocketByteSink() {
	if (state_) {
		state_->stop();
	}
}

SocketByteSink::SocketByteSink(SocketByteSink&&) noexcept = default;

SocketByteSink& SocketByteSink::operator=(SocketByteSink&&) noexcept = default;

void SocketByteSink::write(std::span<const uint8_t> bytes) {
	state_->write(bytes);
}

void SocketByteSink::close() {
	state_->close();
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
		if (stopped) {
			return;
		}
		stopped = true;
		if (receive_in_flight) {
			boost::system::error_code ignored;
			socket.cancel(ignored);
			shutdown_socket();
			while (receive_in_flight) {
				run_one_io();
			}
		} else {
			shutdown_socket();
		}
	}

	void start_receive_if_possible() {
		if (stopped || eof || async_error || receive_in_flight) {
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

				if (stopped && error == boost::asio::error::operation_aborted) {
					return;
				}
				if (error == boost::asio::error::eof || bytes_transferred == 0) {
					eof = true;
					return;
				}
				if (error) {
					async_error = std::make_exception_ptr(make_socket_error("socket read failed", error));
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
		while (!has_readable_data() && !eof && !async_error) {
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

	boost::asio::ip::tcp::socket socket;
	boost::asio::io_context& io_context;
	std::vector<uint8_t> active_buffer;
	std::vector<uint8_t> staged_buffer;
	std::vector<uint8_t> receive_buffer;
	std::size_t active_size = 0;
	std::size_t active_offset = 0;
	std::size_t staged_size = 0;
	bool eof = false;
	bool stopped = false;
	bool receive_in_flight = false;
	std::exception_ptr async_error;
	bool socket_shutdown = false;
};

SocketByteSource::SocketByteSource(boost::asio::ip::tcp::socket socket) : state_(std::make_unique<State>(std::move(socket))) {}

SocketByteSource::~SocketByteSource() {
	if (state_) {
		state_->stop();
	}
}

SocketByteSource::SocketByteSource(SocketByteSource&&) noexcept = default;

SocketByteSource& SocketByteSource::operator=(SocketByteSource&&) noexcept = default;

std::size_t SocketByteSource::read(uint8_t* buffer, std::size_t length) {
	return state_->read(buffer, length);
}

} // namespace soratransport
