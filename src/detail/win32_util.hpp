#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>

#include <malloc.h>
#include <windows.h>

namespace soratransport {

// ── Path conversion utilities ────────────────────────────────────────

inline std::string path_to_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.generic_u8string();
	return {utf8.begin(), utf8.end()};
}

inline std::string path_to_generic_utf8_string(const std::filesystem::path& path) {
	auto utf8 = path.lexically_normal().generic_u8string();
	return {utf8.begin(), utf8.end()};
}

// ── Alignment math utilities ─────────────────────────────────────────

inline std::uint64_t round_down(std::uint64_t value, std::size_t alignment) {
	return value - (value % alignment);
}

inline std::uint64_t round_up(std::uint64_t value, std::size_t alignment) {
	if (value == 0) {
		return 0;
	}
	const auto remainder = value % alignment;
	return remainder == 0 ? value : value + (alignment - remainder);
}

inline bool is_aligned(const void* pointer, std::size_t alignment) {
	return (reinterpret_cast<std::uintptr_t>(pointer) % alignment) == 0;
}

// ── Buffer allocation utilities ──────────────────────────────────────

inline std::shared_ptr<uint8_t> make_aligned_buffer(std::size_t size, std::size_t alignment) {
	auto* pointer = static_cast<uint8_t*>(_aligned_malloc(size, alignment));
	if (pointer == nullptr) {
		throw std::bad_alloc();
	}
	return {pointer, [](uint8_t* value) { _aligned_free(value); }};
}

inline std::shared_ptr<uint8_t> make_heap_buffer(std::size_t size) {
	auto* pointer = new uint8_t[size];
	return {pointer, [](uint8_t* value) { delete[] value; }};
}

// ── Win32 error helpers ──────────────────────────────────────────────

inline std::runtime_error make_win32_error(const std::string& message, DWORD error = ::GetLastError()) {
	return std::runtime_error(message + ": " + std::system_category().message(static_cast<int>(error)));
}

// ── OVERLAPPED slot base class ───────────────────────────────────────
//  Shared scaffolding for ReadSlot (filesystem.cpp) and WriteSlot (io.cpp).

struct OverlappedSlotBase {
	std::shared_ptr<uint8_t> buffer;
	OVERLAPPED overlapped{};
	HANDLE event_handle = nullptr;
	bool in_flight = false;

	OverlappedSlotBase() {
		event_handle = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
		if (event_handle == nullptr) {
			throw make_win32_error("failed to create overlapped event");
		}
		overlapped.hEvent = event_handle;
	}

	~OverlappedSlotBase() {
		if (event_handle != nullptr) {
			::CloseHandle(event_handle);
		}
	}

	OverlappedSlotBase(const OverlappedSlotBase&) = delete;
	OverlappedSlotBase& operator=(const OverlappedSlotBase&) = delete;

	OverlappedSlotBase(OverlappedSlotBase&& other) noexcept
		: buffer(std::move(other.buffer)),
		  overlapped(other.overlapped),
		  event_handle(other.event_handle),
		  in_flight(other.in_flight) {
		other.overlapped = {};
		other.event_handle = nullptr;
		other.in_flight = false;
		overlapped.hEvent = event_handle;
	}

	OverlappedSlotBase& operator=(OverlappedSlotBase&& other) noexcept {
		if (this != &other) {
			if (event_handle != nullptr) {
				::CloseHandle(event_handle);
			}
			buffer = std::move(other.buffer);
			overlapped = other.overlapped;
			event_handle = other.event_handle;
			in_flight = other.in_flight;

			other.overlapped = {};
			other.event_handle = nullptr;
			other.in_flight = false;
			overlapped.hEvent = event_handle;
		}
		return *this;
	}
};

} // namespace soratransport
