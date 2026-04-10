#pragma once

#include "runtime.hpp"

#include <span>

namespace soratransport {

enum class FileIoMode {
	Buffered,
	Direct,
};

struct FileIoAlignmentInfo {
	std::size_t logical_sector_size = 4 * 1024;
	std::size_t physical_sector_size = 4 * 1024;
	std::size_t required_alignment = 4 * 1024;
};

inline constexpr std::size_t kFileIoAlignment = 4 * 1024;

FileIoAlignmentInfo query_file_io_alignment(const std::filesystem::path& path);

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
	void flush_pending_writes();
	void submit_active_write(bool finalize);
	void wait_for_one_write();
	void wait_for_all_writes();
	std::unique_ptr<State> state_;
};

class FileByteSource final : public IByteSource {
public:
	explicit FileByteSource(const std::filesystem::path& input_path, FileIoMode mode = FileIoMode::Buffered);
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
	explicit SocketByteSink(boost::asio::ip::tcp::socket socket);
	~SocketByteSink() override;
	SocketByteSink(const SocketByteSink&) = delete;
	SocketByteSink& operator=(const SocketByteSink&) = delete;
	SocketByteSink(SocketByteSink&&) noexcept;
	SocketByteSink& operator=(SocketByteSink&&) noexcept;
	void listenCancelSignal(CancelEvent& event);
	void write(std::span<const uint8_t> bytes) override;
	void close() override;
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
	explicit SocketByteSource(boost::asio::ip::tcp::socket socket);
	~SocketByteSource() override;
	SocketByteSource(const SocketByteSource&) = delete;
	SocketByteSource& operator=(const SocketByteSource&) = delete;
	SocketByteSource(SocketByteSource&&) noexcept;
	SocketByteSource& operator=(SocketByteSource&&) noexcept;
	void listenCancelSignal(CancelEvent& event);
	std::size_t read(uint8_t* buffer, std::size_t length) override;
	void close_socket();
	void stop();
	bool is_cancelled() const;
	void cancel_pending_work();

private:
	struct State;
	std::unique_ptr<State> state_;
};

} // namespace soratransport
