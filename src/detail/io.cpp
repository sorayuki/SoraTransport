#include "io.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <cstring>
#include <limits>
#include <malloc.h>
#include <stdexcept>
#include <system_error>

#include <windows.h>

namespace soratransport {

namespace {

constexpr std::size_t kDirectIoBufferSize = 4 * 1024 * 1024;

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

std::shared_ptr<uint8_t> make_aligned_buffer(std::size_t size) {
	auto* pointer = static_cast<uint8_t*>(_aligned_malloc(size, kFileIoAlignment));
	if (pointer == nullptr) {
		throw std::bad_alloc();
	}
	return {pointer, [](uint8_t* value) { _aligned_free(value); }};
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

void resize_file_buffered(const std::string& path_text, std::uint64_t size) {
	std::filesystem::path path(path_text);
	const auto handle = ::CreateFileW(
		path.c_str(),
		GENERIC_WRITE,
		0,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
		nullptr);
	if (handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to reopen output file for resize: " + path_text);
	}
	try {
		set_file_size(handle, size, path_text);
		if (!::FlushFileBuffers(handle)) {
			throw make_win32_error("failed to flush resized output file: " + path_text);
		}
		::CloseHandle(handle);
	} catch (...) {
		::CloseHandle(handle);
		throw;
	}
}

} // namespace

struct FileByteSink::State {
	HANDLE handle = INVALID_HANDLE_VALUE;
	std::string path;
	bool closed = false;
	FileIoMode mode = FileIoMode::Buffered;
	std::uint64_t logical_size = 0;
	std::uint64_t physical_size = 0;
	std::shared_ptr<uint8_t> tail_buffer;
	std::size_t tail_size = 0;
	std::shared_ptr<uint8_t> scratch_buffer;
	std::size_t scratch_capacity = 0;
};

struct FileByteSource::State {
	HANDLE handle = INVALID_HANDLE_VALUE;
	std::string path;
	FileIoMode mode = FileIoMode::Buffered;
	std::uint64_t logical_offset = 0;
	std::uint64_t size = 0;
	std::shared_ptr<uint8_t> aligned_buffer;
	std::size_t aligned_buffer_capacity = 0;
	std::size_t aligned_buffer_size = 0;
	std::size_t aligned_buffer_offset = 0;
};

FileByteSink::FileByteSink(const std::filesystem::path& output_path, FileIoMode mode) : state_(std::make_unique<State>()) {
	state_->path = path_to_utf8_string(output_path);
	state_->mode = mode;
	const auto flags = mode == FileIoMode::Direct
		? FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_NO_BUFFERING
		: FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN;
	state_->handle = ::CreateFileW(
		output_path.c_str(),
		GENERIC_WRITE,
		0,
		nullptr,
		CREATE_ALWAYS,
		flags,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open output file: " + state_->path);
	}
	if (mode == FileIoMode::Direct) {
		state_->tail_buffer = make_aligned_buffer(kFileIoAlignment);
		state_->scratch_capacity = kDirectIoBufferSize;
		state_->scratch_buffer = make_aligned_buffer(state_->scratch_capacity);
	}
}

FileByteSink::~FileByteSink() {
	if (state_ && state_->handle != INVALID_HANDLE_VALUE) {
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}
}

void FileByteSink::write(std::span<const uint8_t> bytes) {
	if (!state_ || state_->closed || state_->handle == INVALID_HANDLE_VALUE) {
		throw std::runtime_error("output file is closed");
	}
	if (bytes.empty()) {
		return;
	}
	state_->logical_size += bytes.size();
	if (state_->mode == FileIoMode::Buffered) {
		write_all(state_->handle, bytes, state_->path);
		return;
	}

	auto append_aligned = [&](const uint8_t* data, std::size_t length) {
		if (length == 0) {
			return;
		}
		write_exact_at(state_->handle, data, length, state_->physical_size, state_->path);
		state_->physical_size += length;
	};

	if (state_->tail_size != 0) {
		const auto fill = std::min<std::size_t>(kFileIoAlignment - state_->tail_size, bytes.size());
		std::memcpy(state_->tail_buffer.get() + state_->tail_size, bytes.data(), fill);
		state_->tail_size += fill;
		bytes = bytes.subspan(fill);
		if (state_->tail_size == kFileIoAlignment) {
			append_aligned(state_->tail_buffer.get(), kFileIoAlignment);
			state_->tail_size = 0;
		}
	}

	const auto aligned_bytes = static_cast<std::size_t>(round_down(bytes.size(), kFileIoAlignment));
	if (aligned_bytes > 0) {
		if (is_aligned(bytes.data(), kFileIoAlignment)) {
			append_aligned(bytes.data(), aligned_bytes);
		} else {
			std::size_t copied = 0;
			while (copied < aligned_bytes) {
				const auto chunk = std::min<std::size_t>(aligned_bytes - copied, state_->scratch_capacity);
				std::memcpy(state_->scratch_buffer.get(), bytes.data() + copied, chunk);
				append_aligned(state_->scratch_buffer.get(), chunk);
				copied += chunk;
			}
		}
		bytes = bytes.subspan(aligned_bytes);
	}

	if (!bytes.empty()) {
		std::memcpy(state_->tail_buffer.get(), bytes.data(), bytes.size());
		state_->tail_size = bytes.size();
	}
}

void FileByteSink::close() {
	if (!state_ || state_->closed) {
		return;
	}
	state_->closed = true;
	if (state_->mode == FileIoMode::Direct) {
		if (state_->tail_size != 0) {
			std::memset(state_->tail_buffer.get() + state_->tail_size, 0, kFileIoAlignment - state_->tail_size);
			write_exact_at(state_->handle, state_->tail_buffer.get(), kFileIoAlignment, state_->physical_size, state_->path);
			state_->physical_size += kFileIoAlignment;
			state_->tail_size = 0;
		}
	}
	if (!::FlushFileBuffers(state_->handle)) {
		const auto error = ::GetLastError();
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to flush output file: " + state_->path, error);
	}
	if (!::CloseHandle(state_->handle)) {
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to close output file: " + state_->path);
	}
	state_->handle = INVALID_HANDLE_VALUE;
	if (state_->mode == FileIoMode::Direct) {
		resize_file_buffered(state_->path, state_->logical_size);
	}
}

FileByteSource::FileByteSource(const std::filesystem::path& input_path, FileIoMode mode) : state_(std::make_unique<State>()) {
	state_->path = path_to_utf8_string(input_path);
	state_->mode = mode;
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
		throw make_win32_error("failed to open input file: " + state_->path);
	}
	LARGE_INTEGER file_size{};
	if (!::GetFileSizeEx(state_->handle, &file_size)) {
		const auto error = ::GetLastError();
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
		throw make_win32_error("failed to query input file size: " + state_->path, error);
	}
	state_->size = static_cast<std::uint64_t>(file_size.QuadPart);
	if (mode == FileIoMode::Direct) {
		state_->aligned_buffer_capacity = kDirectIoBufferSize;
		state_->aligned_buffer = make_aligned_buffer(state_->aligned_buffer_capacity);
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
		return read_some(state_->handle, buffer, length, state_->path);
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
		if (remaining >= kFileIoAlignment) {
			request_size = static_cast<std::size_t>(std::min<std::uint64_t>(round_down(remaining, kFileIoAlignment), state_->aligned_buffer_capacity));
			if (request_size == 0) {
				request_size = kFileIoAlignment;
			}
		} else {
			request_size = kFileIoAlignment;
		}

		const auto bytes_read = read_exact_at(state_->handle, state_->aligned_buffer.get(), request_size, state_->logical_offset, state_->path);
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