#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace soratransport::detail2 {

// ============================================================================
// WriteBuffer — 带容量/时间阈值的写缓冲，抽象自 SocketByteSink 内部聚合逻辑
//
// 用法：
//   WriteBuffer buf(capacity, max_delay, [](std::span<const uint8_t> data) {
//       // 实际发送 data
//   });
//   buf.write(some_bytes);
//   buf.flush();  // 也可由析构自动触发
// ============================================================================
class WriteBuffer {
public:
	using FlushCallback = std::function<void(std::span<const uint8_t>)>;

	WriteBuffer(std::size_t capacity_bytes,
	            std::chrono::milliseconds max_buffered_delay,
	            FlushCallback flush_callback);
	~WriteBuffer();

	WriteBuffer(const WriteBuffer&) = delete;
	WriteBuffer& operator=(const WriteBuffer&) = delete;
	WriteBuffer(WriteBuffer&&) = delete;
	WriteBuffer& operator=(WriteBuffer&&) = delete;

	// 追加数据；达到容量或时间阈值时自动 flush
	void write(std::span<const uint8_t> bytes);

	// 立即 flush（即使未满）
	void flush();

	// 当前缓冲字节数
	std::size_t buffered_size() const;

	// 丢弃当前缓冲（不发送）
	void discard();

private:
	void do_flush();

	std::vector<uint8_t> buffer_;
	std::size_t buffered_size_ = 0;
	std::size_t capacity_;
	std::chrono::milliseconds max_delay_;
	std::optional<std::chrono::steady_clock::time_point> fill_started_at_;
	FlushCallback flush_cb_;
	bool flushed_in_dtor_ = false;
};

} // namespace soratransport::detail2
