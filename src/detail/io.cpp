#include "io.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <cstring>
#include <cwchar>
#include <deque>
#include <limits>
#include <malloc.h>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <windows.h>
#include <winioctl.h>

namespace soratransport {

namespace {

constexpr std::size_t kDirectIoBufferSize = 4 * 1024 * 1024;
constexpr std::size_t kBufferedWriteBatchSize = 32 * 1024 * 1024;

std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	return {utf8.begin(), utf8.end()};
}

std::runtime_error make_win32_error(const std::string& message, DWORD error = ::GetLastError()) {
	return std::runtime_error(message + ": " + std::system_category().message(static_cast<int>(error)));
}

std::uint64_t round_down(std::uint64_t value, std::size_t alignment) {
	return value - (value % alignment);
}

std::uint64_t round_up(std::uint64_t value, std::size_t alignment) {
	if (value == 0) {
		return 0;
	}
	const auto remainder = value % alignment;
	return remainder == 0 ? value : value + (alignment - remainder);
}

bool is_aligned(const void* pointer, std::size_t alignment) {
	return (reinterpret_cast<std::uintptr_t>(pointer) % alignment) == 0;
}

std::shared_ptr<uint8_t> make_aligned_buffer(std::size_t size, std::size_t alignment) {
	auto* pointer = static_cast<uint8_t*>(_aligned_malloc(size, alignment));
	if (pointer == nullptr) {
		throw std::bad_alloc();
	}
	return {pointer, [](uint8_t* value) { _aligned_free(value); }};
}

std::shared_ptr<uint8_t> make_heap_buffer(std::size_t size) {
	auto* pointer = new uint8_t[size];
	return {pointer, [](uint8_t* value) { delete[] value; }};
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
		if (!::FlushFileBuffers(handle)) {
			throw make_win32_error("failed to flush resized output file: " + path_to_utf8_string(path));
		}
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
	struct WriteSlot {
		std::shared_ptr<uint8_t> buffer;
		OVERLAPPED overlapped{};
		HANDLE event_handle = nullptr;
		std::size_t size = 0;
		std::uint64_t offset = 0;
		bool in_flight = false;

		WriteSlot() {
			event_handle = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
			if (event_handle == nullptr) {
				throw make_win32_error("failed to create file write event");
			}
			overlapped.hEvent = event_handle;
		}

		~WriteSlot() {
			if (event_handle != nullptr) {
				::CloseHandle(event_handle);
			}
		}

		WriteSlot(const WriteSlot&) = delete;
		WriteSlot& operator=(const WriteSlot&) = delete;

		WriteSlot(WriteSlot&& other) noexcept
			: buffer(std::move(other.buffer)),
			  overlapped(other.overlapped),
			  event_handle(other.event_handle),
			  size(other.size),
			  offset(other.offset),
			  in_flight(other.in_flight) {
			other.overlapped = {};
			other.event_handle = nullptr;
			other.size = 0;
			other.offset = 0;
			other.in_flight = false;
			overlapped.hEvent = event_handle;
		}

		WriteSlot& operator=(WriteSlot&& other) noexcept {
			if (this != &other) {
				if (event_handle != nullptr) {
					::CloseHandle(event_handle);
				}
				buffer = std::move(other.buffer);
				overlapped = other.overlapped;
				event_handle = other.event_handle;
				size = other.size;
				offset = other.offset;
				in_flight = other.in_flight;

				other.overlapped = {};
				other.event_handle = nullptr;
				other.size = 0;
				other.offset = 0;
				other.in_flight = false;
				overlapped.hEvent = event_handle;
			}
			return *this;
		}
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

FileByteSink::FileByteSink(const std::filesystem::path& output_path, FileIoMode mode, std::size_t max_in_flight_write_ops) : state_(std::make_unique<State>()) {
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
	if (!::FlushFileBuffers(state_->handle)) {
		const auto error = ::GetLastError();
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to flush output file: " + state_->display_path, error);
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

SocketByteSink::SocketByteSink(boost::asio::ip::tcp::socket socket) : socket_(std::move(socket)) {}

void SocketByteSink::write(std::span<const uint8_t> bytes) {
	boost::asio::write(socket_, boost::asio::buffer(bytes.data(), bytes.size()));
}

void SocketByteSink::close() {
	if (closed_) {
		return;
	}
	closed_ = true;
	boost::system::error_code ignored;
	socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ignored);
	socket_.close(ignored);
}

SocketByteSource::SocketByteSource(boost::asio::ip::tcp::socket socket) : socket_(std::move(socket)) {}

std::size_t SocketByteSource::read(uint8_t* buffer, std::size_t length) {
	boost::system::error_code error;
	const auto bytes_read = socket_.read_some(boost::asio::buffer(buffer, length), error);
	if (error == boost::asio::error::eof) {
		return 0;
	}
	if (error) {
		throw std::runtime_error("socket read failed: " + error.message());
	}
	return bytes_read;
}

} // namespace soratransport
