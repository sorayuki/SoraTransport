#include "buffered_sender.hpp"

#include <algorithm>
#include <cstring>

namespace soratransport::detail2 {

WriteBuffer::WriteBuffer(std::size_t capacity_bytes,
                         std::chrono::milliseconds max_buffered_delay,
                         FlushCallback flush_callback)
	: capacity_(std::max<std::size_t>(1, capacity_bytes))
	, max_delay_(max_buffered_delay)
	, flush_cb_(std::move(flush_callback)) {
	buffer_.resize(capacity_);
}

WriteBuffer::~WriteBuffer() {
	if (!flushed_in_dtor_ && buffered_size_ > 0) {
		try {
			do_flush();
		} catch (...) {
			// 析构不抛异常
		}
	}
}

void WriteBuffer::write(std::span<const uint8_t> bytes) {
	std::size_t offset = 0;
	while (offset < bytes.size()) {
		const auto available = capacity_ - buffered_size_;
		if (available == 0) {
			do_flush();
			continue;
		}

		const auto was_empty = buffered_size_ == 0;
		const auto now = std::chrono::steady_clock::now();
		const auto chunk = std::min<std::size_t>(available, bytes.size() - offset);
		std::memcpy(buffer_.data() + buffered_size_, bytes.data() + offset, chunk);
		buffered_size_ += chunk;
		offset += chunk;

		if (was_empty) {
			fill_started_at_ = now;
		}

		if (buffered_size_ == capacity_
			|| (fill_started_at_.has_value()
				&& now - *fill_started_at_ >= max_delay_)) {
			do_flush();
		}
	}
}

void WriteBuffer::flush() {
	if (buffered_size_ > 0) {
		do_flush();
	}
}

std::size_t WriteBuffer::buffered_size() const {
	return buffered_size_;
}

void WriteBuffer::discard() {
	buffered_size_ = 0;
	fill_started_at_.reset();
}

void WriteBuffer::do_flush() {
	if (buffered_size_ == 0) {
		return;
	}
	if (flush_cb_) {
		flush_cb_(std::span<const uint8_t>(buffer_.data(), buffered_size_));
	}
	buffered_size_ = 0;
	fill_started_at_.reset();
	flushed_in_dtor_ = true;
}

} // namespace soratransport::detail2
