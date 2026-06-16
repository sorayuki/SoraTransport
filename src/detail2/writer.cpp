#include "writer.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace soratransport::detail2 {

namespace {

std::size_t choose_slot_capacity(const PipelineTuning& tuning, std::optional<std::uint64_t> expected_size) {
	if (!expected_size.has_value() || *expected_size == 0) {
		return tuning.writer_slots.max_slot_bytes;
	}
	return static_cast<std::size_t>(std::max<std::uint64_t>(1, std::min<std::uint64_t>(*expected_size, tuning.writer_slots.max_slot_bytes)));
}

void throw_cancelled_if_needed(bool cancel_requested) {
	if (cancel_requested) {
		throw CancelledError("output file write cancelled");
	}
}

} // namespace

std::shared_ptr<BufferedFileWriter> BufferedFileWriter::create(
	const std::filesystem::path& output_path,
	TaskExecutor& executor,
	PipelineTuning tuning,
	std::optional<std::uint64_t> expected_size,
	const CancelEvent* cancel_event) {
	return std::shared_ptr<BufferedFileWriter>(new BufferedFileWriter(output_path, executor, std::move(tuning), expected_size, cancel_event));
}

BufferedFileWriter::BufferedFileWriter(
	const std::filesystem::path& output_path,
	TaskExecutor& executor,
	PipelineTuning tuning,
	std::optional<std::uint64_t> expected_size,
	const CancelEvent* cancel_event)
	: output_path_(output_path),
	  executor_(executor),
	  tuning_(std::move(tuning)),
	  expected_size_(expected_size),
	  cancel_event_(cancel_event),
	  sink_(std::make_unique<FileByteSink>(output_path, tuning_.max_in_flight_write_ops)),
	  slot_capacity_(choose_slot_capacity(tuning_, expected_size_)),
	  max_slots_(std::max<std::size_t>(1, tuning_.writer_slots.max_slots)) {
	current_slot_.reserve(slot_capacity_);
	if (cancel_event_ != nullptr) {
		auto* mutable_cancel = const_cast<CancelEvent*>(cancel_event_);
		sink_->listenCancelSignal(*mutable_cancel);
		cancel_connection_ = mutable_cancel->connect([this] {
			{
				std::lock_guard lock(mutex_);
				cancel_requested_ = true;
			}
			sink_->cancel_pending_work();
			cv_.notify_all();
		});
	}
}

BufferedFileWriter::~BufferedFileWriter() {
	try {
		close();
	} catch (...) {
	}
}

void BufferedFileWriter::write(std::span<const std::uint8_t> bytes) {
	std::size_t offset = 0;
	while (offset < bytes.size()) {
		std::unique_lock lock(mutex_);
		ensure_writeable_locked();
		throw_cancelled_if_needed(cancel_requested_);

		if (current_slot_.size() == slot_capacity_) {
			enqueue_current_slot_locked(lock);
		}

		const auto writable = std::min(slot_capacity_ - current_slot_.size(), bytes.size() - offset);
		current_slot_.insert(current_slot_.end(), bytes.begin() + offset, bytes.begin() + offset + writable);
		offset += writable;

		if (current_slot_.size() == slot_capacity_) {
			enqueue_current_slot_locked(lock);
		}
	}
}

void BufferedFileWriter::close() {
	std::future<void> drain_future;
	{
		std::unique_lock lock(mutex_);
		if (closed_) {
			return;
		}
		if (background_error_) {
			std::rethrow_exception(background_error_);
		}
		closing_ = true;
		if (!current_slot_.empty()) {
			enqueue_current_slot_locked(lock);
		}
		if (!worker_started_) {
			lock.unlock();
			sink_->close();
			lock.lock();
			closed_ = true;
			return;
		}
		drain_future = std::move(drain_future_);
		cv_.notify_all();
	}

	if (drain_future.valid()) {
		drain_future.get();
	}

	std::lock_guard lock(mutex_);
	if (background_error_) {
		std::rethrow_exception(background_error_);
	}
	closed_ = true;
}

std::size_t BufferedFileWriter::slot_capacity() const {
	return slot_capacity_;
}

std::size_t BufferedFileWriter::max_slots() const {
	return max_slots_;
}

void BufferedFileWriter::ensure_writeable_locked() const {
	if (background_error_) {
		std::rethrow_exception(background_error_);
	}
	if (closed_) {
		throw std::runtime_error("output file writer is closed");
	}
	if (closing_) {
		throw std::runtime_error("output file writer is closing");
	}
}

void BufferedFileWriter::wait_for_available_slot_locked(std::unique_lock<std::mutex>& lock) {
	cv_.wait(lock, [&] {
		return pending_slots_.size() < max_slots_ || background_error_ || cancel_requested_;
	});
	if (background_error_) {
		std::rethrow_exception(background_error_);
	}
	throw_cancelled_if_needed(cancel_requested_);
}

void BufferedFileWriter::enqueue_current_slot_locked(std::unique_lock<std::mutex>& lock) {
	if (current_slot_.empty()) {
		return;
	}
	wait_for_available_slot_locked(lock);
	pending_slots_.push_back(std::move(current_slot_));
	current_slot_.clear();
	current_slot_.reserve(slot_capacity_);
	start_worker_locked();
	cv_.notify_all();
}

void BufferedFileWriter::start_worker_locked() {
	if (worker_started_) {
		return;
	}
	worker_started_ = true;
	auto self = shared_from_this();
	drain_future_ = executor_.submit([self] {
		self->drain_loop();
	});
}

void BufferedFileWriter::drain_loop() {
	try {
		for (;;) {
			std::vector<std::uint8_t> slot;
			bool should_close = false;
			{
				std::unique_lock lock(mutex_);
				cv_.wait(lock, [&] {
					return !pending_slots_.empty() || closing_ || cancel_requested_;
				});
				if (!pending_slots_.empty()) {
					slot = std::move(pending_slots_.front());
					pending_slots_.pop_front();
					cv_.notify_all();
				} else if (closing_) {
					should_close = true;
				} else {
					throw_cancelled_if_needed(cancel_requested_);
					continue;
				}
			}

			throw_cancelled_if_needed(cancel_requested_);
			if (!slot.empty()) {
				sink_->write(std::span<const std::uint8_t>(slot.data(), slot.size()));
				continue;
			}
			if (should_close) {
				sink_->close();
				break;
			}
		}
	} catch (...) {
		std::lock_guard lock(mutex_);
		background_error_ = std::current_exception();
		cv_.notify_all();
		return;
	}

	std::lock_guard lock(mutex_);
	closed_ = true;
	cv_.notify_all();
}

} // namespace soratransport::detail2