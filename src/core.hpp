#pragma once

#include "detail/pipeline.hpp"

#include <stop_token>
#include <tuple>

namespace soratransport {

using StatusText = std::tuple<std::string, std::string>;

struct TransferProgressSnapshot {
	std::uint64_t processed_bytes = 0;
	std::uint64_t processed_files = 0;
	bool completed = false;
	bool failed = false;
	bool cancelled = false;
	StatusText status_text;
};

class TransferProgress {
public:
	void add_processed_bytes(std::uint64_t bytes);
	void add_processed_files(std::uint64_t files = 1);
	void reset(StatusText status_text = {"idle", "空闲"});
	void set_status(StatusText status_text);
	void set_completed(StatusText status_text = {"completed", "已完成"});
	void set_failed(StatusText status_text);
	void set_cancelled(StatusText status_text = {"cancelled", "已取消"});
	TransferProgressSnapshot snapshot() const;

private:
	std::atomic<std::uint64_t> processed_bytes_{0};
	std::atomic<std::uint64_t> processed_files_{0};
	std::atomic<bool> completed_{false};
	std::atomic<bool> failed_{false};
	std::atomic<bool> cancelled_{false};
	mutable std::mutex mutex_;
	StatusText status_text_{"idle", "空闲"};
};

void pack_directory_to_file(
	const std::filesystem::path& source_dir,
	const std::filesystem::path& output_file,
	CompressionMode mode,
	RuntimeOptions options = {},
	CancelEvent* cancel_event = nullptr);
void unpack_file_to_directory(
	const std::filesystem::path& input_file,
	const std::filesystem::path& destination_dir,
	CompressionMode mode,
	RuntimeOptions options = {},
	CancelEvent* cancel_event = nullptr);
void listen_directory(
	const std::filesystem::path& source_dir,
	std::uint16_t port,
	RuntimeOptions options = {},
	const std::shared_ptr<TransferProgress>& progress = {},
	std::atomic<std::uint16_t>* bound_port = nullptr,
	std::stop_token stop_token = {},
	CancelEvent* cancel_event = nullptr);
void receive_directory(
	std::string_view host,
	std::uint16_t port,
	const std::filesystem::path& destination_dir,
	const std::shared_ptr<TransferProgress>& progress = {},
	std::stop_token stop_token = {},
	CancelEvent* cancel_event = nullptr,
	bool keep_connection_open = false);
int run_soratransport_cli(int argc, char** argv);
int run_fasttar_cli(int argc, char** argv);

} // namespace soratransport
