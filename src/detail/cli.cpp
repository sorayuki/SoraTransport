#include "../core.hpp"

#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <vector>

namespace soratransport {

namespace {

std::uint16_t parse_port(std::string_view text) {
	const auto value = std::stoul(std::string(text));
	if (value == 0 || value > std::numeric_limits<std::uint16_t>::max()) {
		throw std::runtime_error("port must be in range 1-65535");
	}
	return static_cast<std::uint16_t>(value);
}

bool ends_with(std::string_view value, std::string_view suffix) {
	return value.size() >= suffix.size() && value.substr(value.size() - suffix.size()) == suffix;
}

CompressionMode infer_fasttar_mode_from_path(std::string_view path_text) {
	if (ends_with(path_text, ".tar.zst") || ends_with(path_text, ".tzst") || ends_with(path_text, ".zst")) {
		return CompressionMode::Zstd;
	}
	return CompressionMode::None;
}

CompressionMode parse_fasttar_mode_option(std::string_view option) {
	if (option == "--zstd") {
		return CompressionMode::Zstd;
	}
	if (option == "--no-compress") {
		return CompressionMode::None;
	}
	throw std::runtime_error("unknown fasttar option: " + std::string(option));
}

FileIoMode parse_file_io_mode_option(std::string_view option) {
	if (option == "--direct-io") {
		return FileIoMode::Direct;
	}
	if (option == "--buffered-io") {
		return FileIoMode::Buffered;
	}
	throw std::runtime_error("unknown file I/O option: " + std::string(option));
}

struct PackUnpackOptions {
	FileIoMode file_io_mode = FileIoMode::Buffered;
	std::optional<CompressionMode> compression_mode;
	std::vector<std::string_view> positional;
};

PackUnpackOptions parse_pack_unpack_options(int argc, char** argv, int first_arg, bool allow_compression_mode) {
	PackUnpackOptions options;
	for (int index = first_arg; index < argc; ++index) {
		const std::string_view argument = argv[index];
		if (argument == "--direct-io" || argument == "--buffered-io") {
			options.file_io_mode = parse_file_io_mode_option(argument);
			continue;
		}
		if (allow_compression_mode && (argument == "--zstd" || argument == "--no-compress")) {
			options.compression_mode = parse_fasttar_mode_option(argument);
			continue;
		}
		options.positional.push_back(argument);
	}
	return options;
}

void validate_fasttar_path_mode(std::string_view path_text, CompressionMode mode, std::string_view verb) {
	const auto is_zstd_path = ends_with(path_text, ".tar.zst") || ends_with(path_text, ".tzst") || ends_with(path_text, ".zst");
	if (mode == CompressionMode::Zstd && !is_zstd_path) {
		throw std::runtime_error(std::string("fasttar ") + std::string(verb) + " with --zstd requires a .tar.zst output/input path");
	}
	if (mode == CompressionMode::None && is_zstd_path) {
		throw std::runtime_error(std::string("fasttar ") + std::string(verb) + " with --no-compress requires a .tar path");
	}
}

void print_soratransport_usage() {
	std::cerr
		<< "Usage:\n"
		<< "  soratransport pack [--direct-io|--buffered-io] <source-dir> <output.tar.zst>\n"
		<< "  soratransport unpack [--direct-io|--buffered-io] <input.tar.zst> <destination-dir>\n"
		<< "  soratransport send <source-dir> <host> <port>\n"
		<< "  soratransport receive <port> <destination-dir>\n";
}

void print_fasttar_usage() {
	std::cerr
		<< "Usage:\n"
		<< "  fasttar pack [--direct-io|--buffered-io] [--zstd|--no-compress] <source-dir> <output.tar|output.tar.zst>\n"
		<< "  fasttar unpack [--direct-io|--buffered-io] [--zstd|--no-compress] <input.tar|input.tar.zst> <destination-dir>\n";
}

} // namespace

int run_soratransport_cli(int argc, char** argv) {
	if (argc < 2) {
		print_soratransport_usage();
		return 1;
	}

	try {
		const std::string command = argv[1];
		if (command == "pack") {
			auto options = parse_pack_unpack_options(argc, argv, 2, false);
			if (options.positional.size() != 2) {
				print_soratransport_usage();
				return 1;
			}
			pack_directory_to_file(options.positional[0], options.positional[1], CompressionMode::Zstd, options.file_io_mode);
			return 0;
		}
		if (command == "unpack") {
			auto options = parse_pack_unpack_options(argc, argv, 2, false);
			if (options.positional.size() != 2) {
				print_soratransport_usage();
				return 1;
			}
			unpack_file_to_directory(options.positional[0], options.positional[1], CompressionMode::Zstd, options.file_io_mode);
			return 0;
		}
		if (command == "send") {
			if (argc != 5) {
				print_soratransport_usage();
				return 1;
			}
			send_directory(argv[2], argv[3], parse_port(argv[4]));
			return 0;
		}
		if (command == "receive") {
			if (argc != 4) {
				print_soratransport_usage();
				return 1;
			}
			receive_directory(parse_port(argv[2]), argv[3]);
			return 0;
		}

		print_soratransport_usage();
		return 1;
	} catch (const std::exception& error) {
		std::cerr << "error: " << error.what() << '\n';
		return 1;
	}
}

int run_fasttar_cli(int argc, char** argv) {
	if (argc < 2) {
		print_fasttar_usage();
		return 1;
	}

	try {
		const std::string command = argv[1];
		if (command == "pack") {
			auto options = parse_pack_unpack_options(argc, argv, 2, true);
			if (options.positional.size() != 2) {
				print_fasttar_usage();
				return 1;
			}

			const auto source_dir = options.positional[0];
			const auto output_path = options.positional[1];
			const auto mode = options.compression_mode.has_value()
				? *options.compression_mode
				: infer_fasttar_mode_from_path(output_path);
			validate_fasttar_path_mode(output_path, mode, "pack");
			pack_directory_to_file(source_dir, output_path, mode, options.file_io_mode);
			return 0;
		}
		if (command == "unpack") {
			auto options = parse_pack_unpack_options(argc, argv, 2, true);
			if (options.positional.size() != 2) {
				print_fasttar_usage();
				return 1;
			}

			const auto input_path = options.positional[0];
			const auto destination_dir = options.positional[1];
			const auto mode = options.compression_mode.has_value()
				? *options.compression_mode
				: infer_fasttar_mode_from_path(input_path);
			validate_fasttar_path_mode(input_path, mode, "unpack");
			unpack_file_to_directory(input_path, destination_dir, mode, options.file_io_mode);
			return 0;
		}

		print_fasttar_usage();
		return 1;
	} catch (const std::exception& error) {
		std::cerr << "error: " << error.what() << '\n';
		return 1;
	}
}

} // namespace soratransport