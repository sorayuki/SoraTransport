#include "windows_helpers.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <set>

#include <shellapi.h>

namespace soratransport {

namespace {

using AdapterAddress = IP_ADAPTER_ADDRESSES_LH;
using UnicastAddress = IP_ADAPTER_UNICAST_ADDRESS_LH;

std::wstring utf8_to_wstring(std::string_view text) {
	if (text.empty()) {
		return {};
	}
	const int size = MultiByteToWideChar(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), nullptr, 0);
	if (size <= 0) {
		throw make_win32_error("failed to convert UTF-8 text to UTF-16");
	}

	std::wstring result(static_cast<std::size_t>(size), L'\0');
	MultiByteToWideChar(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), result.data(), size);
	return result;
}

std::string wstring_to_utf8(std::wstring_view text) {
	if (text.empty()) {
		return {};
	}
	const int size = WideCharToMultiByte(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), nullptr, 0, nullptr, nullptr);
	if (size <= 0) {
		throw make_win32_error("failed to convert UTF-16 text to UTF-8");
	}

	std::string result(static_cast<std::size_t>(size), '\0');
	WideCharToMultiByte(CP_UTF8, 0, text.data(), static_cast<int>(text.size()), result.data(), size, nullptr, nullptr);
	return result;
}

std::string trim_ascii(std::string text) {
	while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front()))) {
		text.erase(text.begin());
	}
	while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
		text.pop_back();
	}
	return text;
}

std::string lowercase_ascii(std::wstring_view text) {
	std::string result;
	result.reserve(text.size());
	for (wchar_t ch : text) {
		const auto narrowed = static_cast<unsigned char>(ch <= 0xff ? ch : '?');
		result.push_back(static_cast<char>(std::tolower(narrowed)));
	}
	return result;
}

bool looks_virtual_adapter(const AdapterAddress& adapter) {
	return adapter.IfType == IF_TYPE_SOFTWARE_LOOPBACK || adapter.IfType == IF_TYPE_TUNNEL;
}

std::string sockaddr_to_host_string(const SOCKADDR* address, DWORD length) {
	char buffer[NI_MAXHOST] = {};
	if (getnameinfo(address, static_cast<socklen_t>(length), buffer, sizeof(buffer), nullptr, 0, NI_NUMERICHOST) != 0) {
		return {};
	}
	return buffer;
}

bool is_usable_unicast(const UnicastAddress& address, std::string_view host) {
	if (address.Address.lpSockaddr == nullptr) {
		return false;
	}
	switch (address.Address.lpSockaddr->sa_family) {
	case AF_INET:
		return host != "127.0.0.1" && !host.starts_with("169.254.");
	case AF_INET6:
		return host != "::1" && !host.starts_with("fe80:");
	default:
		return false;
	}
}

} // namespace

std::optional<std::string> read_clipboard_text() {
	if (!OpenClipboard(nullptr)) {
		return std::nullopt;
	}

	std::optional<std::string> result;
	const HANDLE handle = GetClipboardData(CF_UNICODETEXT);
	if (handle != nullptr) {
		const auto* text = static_cast<const wchar_t*>(GlobalLock(handle));
		if (text != nullptr) {
			result = wstring_to_utf8(text);
			GlobalUnlock(handle);
		}
	}

	CloseClipboard();
	return result;
}

std::vector<std::string> get_utf8_command_line_args() {
	int argc = 0;
	LPWSTR* argv_wide = CommandLineToArgvW(GetCommandLineW(), &argc);
	if (argv_wide == nullptr) {
		throw make_win32_error("CommandLineToArgvW failed");
	}

	struct ArgvReleaser {
		LPWSTR* value;
		~ArgvReleaser() {
			if (value != nullptr) {
				LocalFree(value);
			}
		}
	} releaser{argv_wide};

	std::vector<std::string> args;
	args.reserve(static_cast<std::size_t>(argc));
	for (int index = 0; index < argc; ++index) {
		args.push_back(wstring_to_utf8(argv_wide[index]));
	}
	return args;
}

std::vector<char*> make_argv_view(std::vector<std::string>& args) {
	std::vector<char*> argv;
	argv.reserve(args.size());
	for (auto& arg : args) {
		argv.push_back(arg.data());
	}
	return argv;
}

void write_clipboard_text(std::string_view text) {
	if (!OpenClipboard(nullptr)) {
		throw make_win32_error("failed to open clipboard");
	}

	struct ClipboardCloser {
		~ClipboardCloser() {
			CloseClipboard();
		}
	} close_clipboard;

	if (!EmptyClipboard()) {
		throw make_win32_error("failed to clear clipboard");
	}

	const auto wide_text = utf8_to_wstring(text);
	const std::size_t bytes = (wide_text.size() + 1) * sizeof(wchar_t);
	HGLOBAL handle = GlobalAlloc(GMEM_MOVEABLE, bytes);
	if (handle == nullptr) {
		throw make_win32_error("failed to allocate clipboard buffer");
	}

	void* memory = GlobalLock(handle);
	if (memory == nullptr) {
		GlobalFree(handle);
		throw make_win32_error("failed to lock clipboard buffer");
	}
	std::memcpy(memory, wide_text.c_str(), bytes);
	GlobalUnlock(handle);

	if (SetClipboardData(CF_UNICODETEXT, handle) == nullptr) {
		GlobalFree(handle);
		throw make_win32_error("failed to write clipboard data");
	}
}

std::optional<SoratransUrl> parse_soratrans_url(std::string_view text) {
	auto normalized = trim_ascii(std::string(text));
	static constexpr std::string_view prefix = "soratrans://";
	if (!normalized.starts_with(prefix)) {
		return std::nullopt;
	}

	normalized.erase(0, prefix.size());
	if (!normalized.empty() && normalized.back() == '/') {
		normalized.pop_back();
	}

	SoratransUrl result;
	if (normalized.starts_with('[')) {
		const auto end = normalized.find(']');
		if (end == std::string::npos || end + 2 > normalized.size() || normalized[end + 1] != ':') {
			return std::nullopt;
		}
		result.host = normalized.substr(1, end - 1);
		result.port = static_cast<std::uint16_t>(std::stoul(normalized.substr(end + 2)));
	} else {
		const auto colon = normalized.rfind(':');
		if (colon == std::string::npos) {
			return std::nullopt;
		}
		result.host = normalized.substr(0, colon);
		result.port = static_cast<std::uint16_t>(std::stoul(normalized.substr(colon + 1)));
	}

	result.canonical_text = format_soratrans_url(result.host, result.port);
	return result;
}

std::string format_soratrans_url(std::string_view host, std::uint16_t port) {
	if (host.find(':') != std::string_view::npos) {
		return "soratrans://[" + std::string(host) + "]:" + std::to_string(port) + "/";
	}
	return "soratrans://" + std::string(host) + ":" + std::to_string(port) + "/";
}

std::vector<InterfaceAddress> enumerate_shareable_addresses(std::uint16_t port) {
	ULONG buffer_size = 0;
	GetAdaptersAddresses(AF_UNSPEC, GAA_FLAG_INCLUDE_GATEWAYS, nullptr, nullptr, &buffer_size);
	std::vector<std::byte> buffer(buffer_size);
	auto* head = reinterpret_cast<AdapterAddress*>(buffer.data());
	const auto status = GetAdaptersAddresses(AF_UNSPEC, GAA_FLAG_INCLUDE_GATEWAYS, nullptr, head, &buffer_size);
	if (status != NO_ERROR) {
		throw make_win32_error("GetAdaptersAddresses failed", status);
	}

	std::vector<InterfaceAddress> result;
	std::set<std::string> urls;
	for (auto* adapter = head; adapter != nullptr; adapter = adapter->Next) {
		if (adapter->OperStatus != IfOperStatusUp || adapter->FirstGatewayAddress == nullptr || looks_virtual_adapter(*adapter)) {
			continue;
		}

		for (auto* address = adapter->FirstUnicastAddress; address != nullptr; address = address->Next) {
			const auto host = sockaddr_to_host_string(address->Address.lpSockaddr, address->Address.iSockaddrLength);
			if (host.empty() || !is_usable_unicast(*address, host)) {
				continue;
			}

			InterfaceAddress item;
			item.host = host;
			item.url = format_soratrans_url(item.host, port);
			item.adapter_name = adapter->FriendlyName == nullptr ? L"" : adapter->FriendlyName;
			if (urls.insert(item.url).second) {
				result.push_back(std::move(item));
			}
		}
	}

	std::sort(result.begin(), result.end(), [](const InterfaceAddress& left, const InterfaceAddress& right) {
		return left.url < right.url;
	});
	return result;
}

void flash_window_in_taskbar(HWND window_handle) {
	if (window_handle == nullptr) {
		return;
	}

	FLASHWINFO info{};
	info.cbSize = sizeof(info);
	info.hwnd = window_handle;
	info.dwFlags = FLASHW_TRAY | FLASHW_TIMERNOFG;
	info.uCount = 3;
	FlashWindowEx(&info);
}

} // namespace soratransport
