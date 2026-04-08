#include "core.hpp"
#include "detail/windows_helpers.hpp"

#if defined(_WIN32)
int wmain(int, wchar_t**) {
	auto args_storage = soratransport::get_utf8_command_line_args();
	auto argv = soratransport::make_argv_view(args_storage);
	return soratransport::run_fasttar_cli(static_cast<int>(argv.size()), argv.data());
}
#else
int main(int argc, char** argv) {
	return soratransport::run_fasttar_cli(argc, argv);
}
#endif