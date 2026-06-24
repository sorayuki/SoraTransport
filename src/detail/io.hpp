#pragma once

#include "runtime.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/websocket/stream.hpp>

#include <span>
#include <windows.h>

namespace soratransport {

inline constexpr std::size_t kOverlappedFileReadQueueDepth = 8;

using TransportWebSocket = boost::beast::websocket::stream<boost::asio::ip::tcp::socket>;

class IByteSink {
public:
	virtual ~IByteSink() = default;
	virtual void write(std::span<const uint8_t> bytes) = 0;
	virtual void close() = 0;
};

class IByteSource {
public:
	virtual ~IByteSource() = default;
	virtual std::size_t read(uint8_t* buffer, std::size_t length) = 0;
};

class OverlappedFileReader {
public:
	OverlappedFileReader(
		BufferPool& pool,
		const std::filesystem::path& path,
		std::uint64_t size,
		std::size_t buffer_size,
		HANDLE handle);
	~OverlappedFileReader();
	OverlappedFileReader(const OverlappedFileReader&) = delete;
	OverlappedFileReader& operator=(const OverlappedFileReader&) = delete;
	OverlappedFileReader(OverlappedFileReader&& other);
	OverlappedFileReader& operator=(OverlappedFileReader&& other);
	void listenCancelSignal(CancelEvent& event);
	void start_prefetch(std::size_t max_bytes);
	DataChunk read_next_chunk();
	std::uint64_t offset() const;
	bool eof() const;
	bool is_open() const;
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	bool issue_next_read();
	void prime_prefetch_window(std::size_t max_bytes);
	void initialize_open_state();
	void close();
	std::string path_for_error() const;

	BufferPool* pool_ = nullptr;
	std::unique_ptr<State> state_;
};

class FileByteSink final : public IByteSink {
public:
	explicit FileByteSink(const std::filesystem::path& output_path, std::size_t max_in_flight_write_ops = 1);
	~FileByteSink() override;
	FileByteSink(const FileByteSink&) = delete;
	FileByteSink& operator=(const FileByteSink&) = delete;
	void listenCancelSignal(CancelEvent& event);
	void write(std::span<const uint8_t> bytes) override;
	void close() override;
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	void submit_active_write();
	void wait_for_one_write();
	void wait_for_all_writes();
	std::unique_ptr<State> state_;
};

class FileByteSource final : public IByteSource {
public:
	explicit FileByteSource(const std::filesystem::path& input_path);
	~FileByteSource() override;
	FileByteSource(const FileByteSource&) = delete;
	FileByteSource& operator=(const FileByteSource&) = delete;
	std::size_t read(uint8_t* buffer, std::size_t length) override;

private:
	struct State;
	std::unique_ptr<State> state_;
};

class SocketByteSink final : public IByteSink {
public:
	explicit SocketByteSink(TransportWebSocket websocket, bool enable_keepalive_ping = false);
	~SocketByteSink() override;
	SocketByteSink(const SocketByteSink&) = delete;
	SocketByteSink& operator=(const SocketByteSink&) = delete;
	SocketByteSink(SocketByteSink&&) noexcept;
	SocketByteSink& operator=(SocketByteSink&&) noexcept;
	void listenCancelSignal(CancelEvent& event);
	void send_transport_begin(bool file_comparison = false);
	void send_transport_end();
	void write(std::span<const uint8_t> bytes) override;
	void close() override;

	// 控制通道消息发送（带缓冲聚合）
	// 使用内部 WriteBuffer 聚合，达到容量/时间阈值后自动 flush
	void send_control_message(std::string_view json_payload);
	void flush_control_buffer();

	// 阻塞读取一条控制通道响应消息，返回 JSON 字符串
	// 仅用于文件比较交换等需要读取响应的场景
	std::string await_control_response();

	void check_connection();
	void close_socket();
	void stop();
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	std::unique_ptr<State> state_;
};

class SocketByteSource final : public IByteSource {
public:
	explicit SocketByteSource(TransportWebSocket websocket);
	~SocketByteSource() override;
	SocketByteSource(const SocketByteSource&) = delete;
	SocketByteSource& operator=(const SocketByteSource&) = delete;
	SocketByteSource(SocketByteSource&&) noexcept;
	SocketByteSource& operator=(SocketByteSource&&) noexcept;
	void listenCancelSignal(CancelEvent& event);
	bool await_transport_begin();

	// 查询上次 await_transport_begin() 收到的 transport_begin 是否包含 file_comparison 标记
	bool transport_has_file_comparison() const;

	std::size_t read(uint8_t* buffer, std::size_t length) override;

	// 控制通道消息接收
	// 返回 false 表示连接关闭，true 表示成功读取一条控制消息
	bool await_control_message(std::string& json_out);

	// 控制通道消息发送（用于接收端回传 file_info_diff 等响应）
	void send_control_message(std::string_view json_payload);

	void close_socket();
	void stop();
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	std::unique_ptr<State> state_;
};

} // namespace soratransport
