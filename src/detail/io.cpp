#include "io.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <windows.h>

namespace soratransport {

namespace {

constexpr std::size_t kMappedReadChunkSize = 4 * 1024 * 1024;

std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	return {utf8.begin(), utf8.end()};
}

std::runtime_error make_win32_error(const std::string& message, DWORD error = ::GetLastError()) {
	return std::runtime_error(message + ": " + std::system_category().message(static_cast<int>(error)));
}

std::uint64_t align_down(std::uint64_t value, std::uint64_t alignment) {
	return value - (value % alignment);
}

} // namespace

struct FileByteSourceState {
	struct PrefetchedChunk {
		std::shared_ptr<uint8_t> data;
		std::uint64_t offset = 0;
		std::size_t length = 0;
	};

	explicit FileByteSourceState(std::filesystem::path input_path) : path(std::move(input_path)) {}

	std::filesystem::path path;
	HANDLE handle = INVALID_HANDLE_VALUE;
	HANDLE mapping_handle = nullptr;
	std::uint64_t file_size = 0;
	DWORD allocation_granularity = 0;
	std::uint64_t next_prefetch_offset = 0;
	PrefetchedChunk current_chunk;
	std::size_t current_chunk_consumed = 0;
	std::future<PrefetchedChunk> next_chunk_future;
	bool next_chunk_in_flight = false;
};

namespace {

FileByteSourceState::PrefetchedChunk map_prefetched_chunk(
	HANDLE mapping_handle,
	const std::filesystem::path& path,
	std::uint64_t offset,
	std::uint64_t file_size,
	DWORD allocation_granularity) {
	FileByteSourceState::PrefetchedChunk chunk;
	if (offset >= file_size) {
		return chunk;
	}

	chunk.offset = offset;
	chunk.length = static_cast<std::size_t>(std::min<std::uint64_t>(kMappedReadChunkSize, file_size - offset));
	const auto view_offset = align_down(offset, allocation_granularity);
	const auto offset_within_view = static_cast<std::size_t>(offset - view_offset);
	const auto view_length = offset_within_view + chunk.length;
	auto* mapped_view = static_cast<uint8_t*>(::MapViewOfFile(
		mapping_handle,
		FILE_MAP_READ,
		static_cast<DWORD>((view_offset >> 32) & 0xffffffffull),
		static_cast<DWORD>(view_offset & 0xffffffffull),
		view_length));
	if (mapped_view == nullptr) {
		throw make_win32_error("failed to map input file: " + path_to_utf8_string(path));
	}

	auto mapped_owner = std::shared_ptr<uint8_t>(mapped_view, [](uint8_t* pointer) {
		if (pointer != nullptr) {
			::UnmapViewOfFile(pointer);
		}
	});

	WIN32_MEMORY_RANGE_ENTRY range{};
	range.VirtualAddress = mapped_view + offset_within_view;
	range.NumberOfBytes = chunk.length;
	if (!::PrefetchVirtualMemory(::GetCurrentProcess(), 1, &range, 0)) {
		const auto error = ::GetLastError();
		if (error != ERROR_NOT_SUPPORTED && error != ERROR_CALL_NOT_IMPLEMENTED) {
			mapped_owner.reset();
			throw make_win32_error("failed to prefetch input file: " + path_to_utf8_string(path), error);
		}
	}

	chunk.data = {mapped_owner, mapped_view + offset_within_view};
	return chunk;
}

void issue_next_prefetch(FileByteSourceState& state) {
	if (state.next_chunk_in_flight || state.next_prefetch_offset >= state.file_size || state.mapping_handle == nullptr) {
		return;
	}

	state.next_chunk_future = std::async(
		std::launch::async,
		[mapping_handle = state.mapping_handle,
		 path = state.path,
		 offset = state.next_prefetch_offset,
		 file_size = state.file_size,
		 allocation_granularity = state.allocation_granularity] {
			return map_prefetched_chunk(mapping_handle, path, offset, file_size, allocation_granularity);
		});
	state.next_chunk_in_flight = true;
	state.next_prefetch_offset += std::min<std::uint64_t>(kMappedReadChunkSize, state.file_size - state.next_prefetch_offset);
}

bool ensure_current_chunk(FileByteSourceState& state) {
	if (state.current_chunk_consumed < state.current_chunk.length) {
		return true;
	}

	state.current_chunk = {};
	state.current_chunk_consumed = 0;
	if (!state.next_chunk_in_flight) {
		return false;
	}

	state.current_chunk = state.next_chunk_future.get();
	state.next_chunk_future = {};
	state.next_chunk_in_flight = false;
	if (state.current_chunk.length == 0) {
		return false;
	}

	issue_next_prefetch(state);
	return true;
}

} // namespace

FileByteSink::FileByteSink(const std::filesystem::path& output_path) : output_(output_path, std::ios::binary | std::ios::trunc) {
	if (!output_) {
		throw std::runtime_error("failed to open output file: " + output_path.string());
	}
}

void FileByteSink::write(std::span<const uint8_t> bytes) {
	output_.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
	if (!output_) {
		throw std::runtime_error("failed to write output file");
	}
}

void FileByteSink::close() {
	output_.flush();
	if (!output_) {
		throw std::runtime_error("failed to flush output file");
	}
	output_.close();
}


FileByteSource::FileByteSource(const std::filesystem::path& input_path) : state_(std::make_unique<FileByteSourceState>(input_path)) {
	state_->handle = ::CreateFileW(
		input_path.c_str(),
		GENERIC_READ,
		FILE_SHARE_READ,
		nullptr,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
		nullptr);
	if (state_->handle == INVALID_HANDLE_VALUE) {
		throw make_win32_error("failed to open input file: " + path_to_utf8_string(input_path));
	}

	LARGE_INTEGER size{};
	if (!::GetFileSizeEx(state_->handle, &size)) {
		const auto error = ::GetLastError();
		close();
		throw make_win32_error("failed to query input file size: " + path_to_utf8_string(input_path), error);
	}
	state_->file_size = static_cast<std::uint64_t>(size.QuadPart);

	SYSTEM_INFO system_info{};
	::GetSystemInfo(&system_info);
	state_->allocation_granularity = std::max<DWORD>(1, system_info.dwAllocationGranularity);

	if (state_->file_size == 0) {
		return;
	}

	state_->mapping_handle = ::CreateFileMappingW(state_->handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
	if (state_->mapping_handle == nullptr) {
		const auto error = ::GetLastError();
		close();
		throw make_win32_error("failed to create input file mapping: " + path_to_utf8_string(input_path), error);
	}

	issue_next_prefetch(*state_);
}

FileByteSource::~FileByteSource() {
	close();
}

void FileByteSource::close() {
	if (!state_) {
		return;
	}

	if (state_->next_chunk_in_flight && state_->next_chunk_future.valid()) {
		try {
			auto pending_chunk = state_->next_chunk_future.get();
			pending_chunk.data.reset();
		} catch (...) {
		}
		state_->next_chunk_in_flight = false;
	}
	state_->next_chunk_future = {};
	state_->current_chunk.data.reset();
	state_->current_chunk = {};
	state_->current_chunk_consumed = 0;

	if (state_->mapping_handle != nullptr) {
		::CloseHandle(state_->mapping_handle);
		state_->mapping_handle = nullptr;
	}
	if (state_->handle != INVALID_HANDLE_VALUE) {
		::CloseHandle(state_->handle);
		state_->handle = INVALID_HANDLE_VALUE;
	}
}

std::size_t FileByteSource::read(uint8_t* buffer, std::size_t length) {
	if (!state_) {
		throw std::runtime_error("input file source is closed");
	}
	if (length == 0 || state_->handle == INVALID_HANDLE_VALUE) {
		return 0;
	}

	std::size_t total_bytes_read = 0;
	while (total_bytes_read < length) {
		if (!ensure_current_chunk(*state_)) {
			break;
		}

		const auto remaining_in_chunk = state_->current_chunk.length - state_->current_chunk_consumed;
		const auto bytes_to_copy = std::min(length - total_bytes_read, remaining_in_chunk);
		std::memcpy(
			buffer + total_bytes_read,
			state_->current_chunk.data.get() + state_->current_chunk_consumed,
			bytes_to_copy);
		state_->current_chunk_consumed += bytes_to_copy;
		total_bytes_read += bytes_to_copy;
	}

	return total_bytes_read;
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