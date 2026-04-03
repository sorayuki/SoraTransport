#pragma once

#include "detail/pipeline.hpp"

namespace soratransport {

void pack_directory_to_file(
	const std::filesystem::path& source_dir,
	const std::filesystem::path& output_file,
	CompressionMode mode,
	FileIoMode file_io_mode = FileIoMode::Buffered,
	RuntimeOptions options = {});
void unpack_file_to_directory(
	const std::filesystem::path& input_file,
	const std::filesystem::path& destination_dir,
	CompressionMode mode,
	FileIoMode file_io_mode = FileIoMode::Buffered,
	RuntimeOptions options = {});
void send_directory(
	const std::filesystem::path& source_dir,
	std::string_view host,
	std::uint16_t port,
	RuntimeOptions options = {});
void receive_directory(std::uint16_t port, const std::filesystem::path& destination_dir);
int run_soratransport_cli(int argc, char** argv);
int run_fasttar_cli(int argc, char** argv);

} // namespace soratransport
