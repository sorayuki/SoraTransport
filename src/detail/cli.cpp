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

std::size_t parse_mebibytes(std::string_view text, std::string_view option_name) {
	const auto value = std::stoull(std::string(text));
	if (value < 64) {
		throw std::runtime_error(std::string(option_name) + " must be at least 64 MiB");
	}
	const auto max_mib = std::numeric_limits<std::size_t>::max() / (1024ull * 1024ull);
	if (value > max_mib) {
		throw std::runtime_error(std::string(option_name) + " is too large");
	}
	return static_cast<std::size_t>(value) * 1024ull * 1024ull;
}

std::size_t parse_positive_count(std::string_view text, std::string_view option_name) {
	const auto value = std::stoull(std::string(text));
	if (value == 0) {
		throw std::runtime_error(std::string(option_name) + " must be at least 1");
	}
	if (value > std::numeric_limits<std::size_t>::max()) {
		throw std::runtime_error(std::string(option_name) + " is too large");
	}
	return static_cast<std::size_t>(value);
}

int parse_compression_level(std::string_view text, std::string_view option_name) {
	const auto value = std::stoi(std::string(text));
	if (value < -131072 || value > 22) {
		throw std::runtime_error(std::string(option_name) + " must be in range -131072..22");
	}
	return value;
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
	if (option == "-z") {
		return CompressionMode::Zstd;
	}
	if (option == "-n") {
		return CompressionMode::None;
	}
	throw std::runtime_error("unknown fasttar option: " + std::string(option));
}

FileIoMode parse_file_io_mode_option(std::string_view option) {
	if (option == "-d") {
		return FileIoMode::Direct;
	}
	if (option == "-b") {
		return FileIoMode::Buffered;
	}
	throw std::runtime_error("unknown file I/O option: " + std::string(option));
}

struct PackUnpackOptions {
	FileIoMode file_io_mode = FileIoMode::Buffered;
	std::optional<CompressionMode> compression_mode;
	RuntimeOptions runtime_options;
	std::vector<std::string_view> positional;
};

bool try_parse_runtime_option(RuntimeOptions& rt, std::string_view argument, int argc, char** argv, int& index) {
	if (argument == "-r") {
		++index;
		if (index >= argc) {
			throw std::runtime_error(std::string(argument) + " requires a value");
		}
		rt.max_in_flight_read_bytes = parse_mebibytes(argv[index], argument);
		return true;
	}
	if (argument == "-w") {
		++index;
		if (index >= argc) {
			throw std::runtime_error(std::string(argument) + " requires a value");
		}
		rt.max_in_flight_write_ops = parse_positive_count(argv[index], argument);
		return true;
	}
	if (argument == "-l") {
		++index;
		if (index >= argc) {
			throw std::runtime_error(std::string(argument) + " requires a value");
		}
		rt.compression_level = parse_compression_level(argv[index], argument);
		return true;
	}
	return false;
}

PackUnpackOptions parse_pack_unpack_options(int argc, char** argv, int first_arg, bool allow_compression_mode) {
	PackUnpackOptions options;
	for (int index = first_arg; index < argc; ++index) {
		const std::string_view argument = argv[index];
		if (argument == "-d" || argument == "-b") {
			options.file_io_mode = parse_file_io_mode_option(argument);
			continue;
		}
		if (allow_compression_mode && (argument == "-z" || argument == "-n")) {
			options.compression_mode = parse_fasttar_mode_option(argument);
			continue;
		}
		if (try_parse_runtime_option(options.runtime_options, argument, argc, argv, index)) {
			continue;
		}
		options.positional.push_back(argument);
	}
	return options;
}

struct SendOptions {
	RuntimeOptions runtime_options;
	std::vector<std::string_view> positional;
};

SendOptions parse_send_options(int argc, char** argv, int first_arg) {
	SendOptions options;
	for (int index = first_arg; index < argc; ++index) {
		const std::string_view argument = argv[index];
		if (try_parse_runtime_option(options.runtime_options, argument, argc, argv, index)) {
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
		<< "  soratransport pack [-d|-b] [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <output.tar.zst>\n"
		<< "  soratransport unpack [-d|-b] [-r <MiB>] [-w <count>] <input.tar.zst> <destination-dir>\n"
		<< "  soratransport send [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <host> <port>\n"
		<< "  soratransport receive <port> <destination-dir>\n"
		<< "\n"
		<< "Options:\n"
		<< "  -d          Use direct I/O for file reads when applicable\n"
		<< "  -b          Use buffered I/O for file reads\n"
		<< "  -r <MiB>    Max in-flight read budget in MiB\n"
		<< "  -w <count>  Max in-flight output write operations\n"
		<< "  -l <level>  Zstd compression level, range -131072..22\n";
}

void print_fasttar_usage() {
	std::cerr
		<< "Usage:\n"
		<< "  fasttar pack [-d|-b] [-z|-n] [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <output.tar|output.tar.zst>\n"
		<< "  fasttar unpack [-d|-b] [-z|-n] [-r <MiB>] [-w <count>] <input.tar|input.tar.zst> <destination-dir>\n"
		<< "\n"
		<< "Options:\n"
		<< "  -d          Use direct I/O for file reads when applicable\n"
		<< "  -b          Use buffered I/O for file reads\n"
		<< "  -z          Use zstd compression\n"
		<< "  -n          Disable compression and use raw tar\n"
		<< "  -r <MiB>    Max in-flight read budget in MiB\n"
		<< "  -w <count>  Max in-flight output write operations\n"
		<< "  -l <level>  Zstd compression level, range -131072..22\n";
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
			pack_directory_to_file(options.positional[0], options.positional[1], CompressionMode::Zstd, options.file_io_mode, options.runtime_options);
			return 0;
		}
		if (command == "unpack") {
			auto options = parse_pack_unpack_options(argc, argv, 2, false);
			if (options.positional.size() != 2) {
				print_soratransport_usage();
				return 1;
			}
			unpack_file_to_directory(options.positional[0], options.positional[1], CompressionMode::Zstd, options.file_io_mode, options.runtime_options);
			return 0;
		}
		if (command == "send") {
			auto options = parse_send_options(argc, argv, 2);
			if (options.positional.size() != 3) {
				print_soratransport_usage();
				return 1;
			}
			send_directory(options.positional[0], options.positional[1], parse_port(options.positional[2]), options.runtime_options);
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
			if (!options.compression_mode.has_value()) {
				validate_fasttar_path_mode(output_path, mode, "pack");
			}
			pack_directory_to_file(source_dir, output_path, mode, options.file_io_mode, options.runtime_options);
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
			if (!options.compression_mode.has_value()) {
				validate_fasttar_path_mode(input_path, mode, "unpack");
			}
			unpack_file_to_directory(input_path, destination_dir, mode, options.file_io_mode, options.runtime_options);
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
