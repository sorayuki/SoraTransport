#include "core.hpp"

namespace soratransport {

void TransferProgress::add_processed_bytes(std::uint64_t bytes) {
	processed_bytes_.fetch_add(bytes, std::memory_order_relaxed);
}

void TransferProgress::add_processed_files(std::uint64_t files) {
	processed_files_.fetch_add(files, std::memory_order_relaxed);
}

void TransferProgress::reset(StatusText status_text) {
	processed_bytes_.store(0, std::memory_order_relaxed);
	processed_files_.store(0, std::memory_order_relaxed);
	completed_.store(false, std::memory_order_relaxed);
	failed_.store(false, std::memory_order_relaxed);
	cancelled_.store(false, std::memory_order_relaxed);
	std::lock_guard lock(mutex_);
	status_text_ = std::move(status_text);
}

void TransferProgress::set_status(StatusText status_text) {
	std::lock_guard lock(mutex_);
	status_text_ = std::move(status_text);
}

void TransferProgress::set_completed(StatusText status_text) {
	{
		std::lock_guard lock(mutex_);
		status_text_ = std::move(status_text);
	}
	completed_.store(true, std::memory_order_relaxed);
	failed_.store(false, std::memory_order_relaxed);
	cancelled_.store(false, std::memory_order_relaxed);
}

void TransferProgress::set_failed(StatusText status_text) {
	{
		std::lock_guard lock(mutex_);
		status_text_ = std::move(status_text);
	}
	failed_.store(true, std::memory_order_relaxed);
	completed_.store(true, std::memory_order_relaxed);
	cancelled_.store(false, std::memory_order_relaxed);
}

void TransferProgress::set_cancelled(StatusText status_text) {
	{
		std::lock_guard lock(mutex_);
		status_text_ = std::move(status_text);
	}
	cancelled_.store(true, std::memory_order_relaxed);
	failed_.store(false, std::memory_order_relaxed);
	completed_.store(true, std::memory_order_relaxed);
}

TransferProgressSnapshot TransferProgress::snapshot() const {
	std::lock_guard lock(mutex_);
	return TransferProgressSnapshot{
		.processed_bytes = processed_bytes_.load(std::memory_order_relaxed),
		.processed_files = processed_files_.load(std::memory_order_relaxed),
		.completed = completed_.load(std::memory_order_relaxed),
		.failed = failed_.load(std::memory_order_relaxed),
		.cancelled = cancelled_.load(std::memory_order_relaxed),
		.status_text = status_text_,
	};
}

} // namespace soratransport
