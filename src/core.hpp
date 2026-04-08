#pragma once

#include "detail/pipeline.hpp"

namespace soratransport {

struct TransferProgressSnapshot {
	std::uint64_t processed_bytes = 0;
	std::uint64_t processed_files = 0;
	bool completed = false;
	bool failed = false;
	std::string status_text;
};

class TransferProgress {
public:
	void add_processed_bytes(std::uint64_t bytes);
	void add_processed_files(std::uint64_t files = 1);
	void set_status(std::string status_text);
	void set_completed(std::string status_text = "completed");
	void set_failed(std::string status_text);
	TransferProgressSnapshot snapshot() const;

private:
	std::atomic<std::uint64_t> processed_bytes_{0};
	std::atomic<std::uint64_t> processed_files_{0};
	std::atomic<bool> completed_{false};
	std::atomic<bool> failed_{false};
	mutable std::mutex mutex_;
	std::string status_text_ = "idle";
};

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
void listen_directory(
	const std::filesystem::path& source_dir,
	std::uint16_t port,
	RuntimeOptions options = {},
	const std::shared_ptr<TransferProgress>& progress = {},
	std::atomic<std::uint16_t>* bound_port = nullptr);
void receive_directory(
	std::string_view host,
	std::uint16_t port,
	const std::filesystem::path& destination_dir,
	const std::shared_ptr<TransferProgress>& progress = {});
int run_soratransport_cli(int argc, char** argv);
int run_fasttar_cli(int argc, char** argv);

} // namespace soratransport
