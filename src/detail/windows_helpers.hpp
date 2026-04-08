#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>

#include "win32_util.hpp"

namespace soratransport {

struct SoratransUrl {
	std::string host;
	std::uint16_t port = 0;
	std::string canonical_text;
};

struct InterfaceAddress {
	std::string host;
	std::string url;
	std::wstring adapter_name;
};

std::optional<std::string> read_clipboard_text();
void write_clipboard_text(std::string_view text);
std::vector<std::string> get_utf8_command_line_args();
std::vector<char*> make_argv_view(std::vector<std::string>& args);
std::optional<SoratransUrl> parse_soratrans_url(std::string_view text);
std::string format_soratrans_url(std::string_view host, std::uint16_t port);
std::vector<InterfaceAddress> enumerate_shareable_addresses(std::uint16_t port);
void flash_window_in_taskbar(HWND window_handle);

} // namespace soratransport
