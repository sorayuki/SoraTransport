#pragma once

#include "config.hpp"
#include "infra.hpp"

#include "../detail/io.hpp"

#include <future>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace soratransport::detail2 {

class BufferedFileWriter : public std::enable_shared_from_this<BufferedFileWriter> {
public:
	static std::shared_ptr<BufferedFileWriter> create(
		const std::filesystem::path& output_path,
		TaskExecutor& executor,
		PipelineTuning tuning,
		std::optional<std::uint64_t> expected_size = std::nullopt,
		const CancelEvent* cancel_event = nullptr);

	~BufferedFileWriter();
	BufferedFileWriter(const BufferedFileWriter&) = delete;
	BufferedFileWriter& operator=(const BufferedFileWriter&) = delete;

	void write(std::span<const std::uint8_t> bytes);
	void close();

	std::size_t slot_capacity() const;
	std::size_t max_slots() const;

private:
	BufferedFileWriter(
		const std::filesystem::path& output_path,
		TaskExecutor& executor,
		PipelineTuning tuning,
		std::optional<std::uint64_t> expected_size,
		const CancelEvent* cancel_event);

	void ensure_writeable_locked() const;
	void wait_for_available_slot_locked(std::unique_lock<std::mutex>& lock);
	void enqueue_current_slot_locked(std::unique_lock<std::mutex>& lock);
	void start_worker_locked();
	void drain_loop();

	std::filesystem::path output_path_;
	TaskExecutor& executor_;
	PipelineTuning tuning_;
	std::optional<std::uint64_t> expected_size_;
	const CancelEvent* cancel_event_ = nullptr;
	std::unique_ptr<FileByteSink> sink_;
	std::size_t slot_capacity_ = 0;
	std::size_t max_slots_ = 1;
	std::vector<std::uint8_t> current_slot_;
	std::deque<std::vector<std::uint8_t>> pending_slots_;
	mutable std::mutex mutex_;
	std::condition_variable cv_;
	std::future<void> drain_future_;
	std::exception_ptr background_error_;
	bool worker_started_ = false;
	bool closing_ = false;
	bool closed_ = false;
	bool cancel_requested_ = false;
	boost::signals2::scoped_connection cancel_connection_;
};

} // namespace soratransport::detail2