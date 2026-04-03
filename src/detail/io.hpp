#pragma once

#include "runtime.hpp"

#include <span>

namespace soratransport {

enum class FileIoMode {
	Buffered,
	Direct,
};

inline constexpr std::size_t kFileIoAlignment = 4 * 1024;
inline constexpr std::size_t kFileIoPreallocationGranularity = 64 * 1024 * 1024;

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
	explicit FileByteSink(const std::filesystem::path& output_path, FileIoMode mode = FileIoMode::Buffered);
	~FileByteSink() override;
	FileByteSink(const FileByteSink&) = delete;
	FileByteSink& operator=(const FileByteSink&) = delete;
	void write(std::span<const uint8_t> bytes) override;
	void close() override;

private:
	struct State;
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
	void write(std::span<const uint8_t> bytes) override;
	void close() override;

private:
	boost::asio::ip::tcp::socket socket_;
	bool closed_ = false;
};

class SocketByteSource final : public IByteSource {
public:
	explicit SocketByteSource(boost::asio::ip::tcp::socket socket);
	std::size_t read(uint8_t* buffer, std::size_t length) override;

private:
	boost::asio::ip::tcp::socket socket_;
};

} // namespace soratransport