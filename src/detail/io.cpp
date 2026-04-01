#include "io.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include <stdexcept>

namespace soratransport {

FileByteSink::FileByteSink(const std::filesystem::path& output_path) : output_(output_path, std::ios::binary | std::ios::trunc) {
	if (!output_) {
		throw std::runtime_error("failed to open output file: " + output_path.string());
	}
}

void FileByteSink::write(std::span<const uint8_t> bytes) {
	output_.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
	if (!output_) {
		throw std::runtime_error("failed to write output file");
	}
}

void FileByteSink::close() {
	output_.flush();
	if (!output_) {
		throw std::runtime_error("failed to flush output file");
	}
	output_.close();
}

FileByteSource::FileByteSource(const std::filesystem::path& input_path) : input_(input_path, std::ios::binary) {
	if (!input_) {
		throw std::runtime_error("failed to open input file: " + input_path.string());
	}
}

std::size_t FileByteSource::read(uint8_t* buffer, std::size_t length) {
	input_.read(reinterpret_cast<char*>(buffer), static_cast<std::streamsize>(length));
	const auto bytes_read = static_cast<std::size_t>(input_.gcount());
	if (bytes_read == 0 && input_.bad()) {
		throw std::runtime_error("failed to read input file");
	}
	return bytes_read;
}

SocketByteSink::SocketByteSink(boost::asio::ip::tcp::socket socket) : socket_(std::move(socket)) {}

void SocketByteSink::write(std::span<const uint8_t> bytes) {
	boost::asio::write(socket_, boost::asio::buffer(bytes.data(), bytes.size()));
}

void SocketByteSink::close() {
	if (closed_) {
		return;
	}
	closed_ = true;
	boost::system::error_code ignored;
	socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ignored);
	socket_.close(ignored);
}

SocketByteSource::SocketByteSource(boost::asio::ip::tcp::socket socket) : socket_(std::move(socket)) {}

std::size_t SocketByteSource::read(uint8_t* buffer, std::size_t length) {
	boost::system::error_code error;
	const auto bytes_read = socket_.read_some(boost::asio::buffer(buffer, length), error);
	if (error == boost::asio::error::eof) {
		return 0;
	}
	if (error) {
		throw std::runtime_error("socket read failed: " + error.message());
	}
	return bytes_read;
}

} // namespace soratransport