#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

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

inline std::string utf16_to_utf8(std::wstring_view text) {
	if (text.empty()) {
		return {};
	}
	const int size = ::WideCharToMultiByte(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), nullptr, 0, nullptr, nullptr);
	if (size <= 0) {
		throw std::runtime_error("failed to convert UTF-16 text to UTF-8");
	}

	std::string result(static_cast<std::size_t>(size), '\0');
	::WideCharToMultiByte(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), result.data(), size, nullptr, nullptr);
	return result;
}

inline std::shared_ptr<uint8_t> make_heap_buffer(std::size_t size) {
	auto* pointer = new uint8_t[size];
	return {pointer, [](uint8_t* value) { delete[] value; }};
}

// ── Win32 error helpers ──────────────────────────────────────────────

inline std::string win32_error_message_utf8(DWORD error) {
	LPWSTR buffer = nullptr;
	const DWORD size = ::FormatMessageW(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		error,
		0,
		reinterpret_cast<LPWSTR>(&buffer),
		0,
		nullptr);
	if (size == 0 || buffer == nullptr) {
		return std::system_category().message(static_cast<int>(error));
	}

	std::wstring message(buffer, size);
	::LocalFree(buffer);
	while (!message.empty()
		&& (message.back() == L'\r'
			|| message.back() == L'\n'
			|| message.back() == L' '
			|| message.back() == L'\t')) {
		message.pop_back();
	}
	return utf16_to_utf8(message);
}

inline std::runtime_error make_win32_error(const std::string& message, DWORD error = ::GetLastError()) {
	return std::runtime_error(message + ": " + win32_error_message_utf8(error));
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
