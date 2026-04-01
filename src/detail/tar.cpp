#include "pipeline.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <stdexcept>

namespace soratransport {

namespace {

struct TarWriteContext {
	BoundedQueue<DataChunk>* out_tar = nullptr;
	ConcurrentDataChunkChannel* out_channel = nullptr;
	BufferPool* pool = nullptr;
	std::uint64_t offset = 0;
};

struct TarReadContext {
	BoundedQueue<DataChunk>* in_tar = nullptr;
	ConcurrentDataChunkChannel* in_channel = nullptr;
	std::optional<DataChunk> current_chunk;
	std::uint64_t offset = 0;
};

int permissions_to_mode(std::filesystem::perms permissions) {
	return static_cast<int>(permissions) & 0777;
}

void throw_archive_error(struct archive* handle, std::string_view prefix) {
	throw std::runtime_error(std::string(prefix) + ": " + archive_error_string(handle));
}

std::filesystem::path normalize_relative_path(const std::filesystem::path& input) {
	auto normalized = input.lexically_normal();
	if (normalized.empty()) {
		throw std::runtime_error("archive entry path is empty");
	}
	if (normalized.is_absolute()) {
		throw std::runtime_error("archive entry path must be relative");
	}
	for (const auto& part : normalized) {
		if (part == "..") {
			throw std::runtime_error("archive entry path escapes destination");
		}
	}
	return normalized;
}

void send_chunk_blocking(ConcurrentDataChunkChannel& channel, DataChunk chunk) {
	auto result = std::make_shared<std::promise<boost::system::error_code>>();
	auto future = result->get_future();
	channel.async_send(boost::system::error_code{}, std::move(chunk),
		[result](boost::system::error_code error) {
			result->set_value(error);
		});

	const auto error = future.get();
	if (error) {
		throw std::runtime_error("channel send failed: " + error.message());
	}
}

std::optional<DataChunk> receive_chunk_blocking(ConcurrentDataChunkChannel& channel) {
	struct ReceiveResult {
		boost::system::error_code error;
		DataChunk chunk;
	};

	auto result = std::make_shared<std::promise<ReceiveResult>>();
	auto future = result->get_future();
	channel.async_receive([result](boost::system::error_code error, DataChunk chunk) mutable {
		result->set_value(ReceiveResult{error, std::move(chunk)});
	});

	auto received = future.get();
	if (received.error) {
		if (!channel.is_open()) {
			return std::nullopt;
		}
		throw std::runtime_error("channel receive failed: " + received.error.message());
	}
	return std::move(received.chunk);
}

} // namespace

TarPacker::TarPacker(BufferPool& pool, RuntimeExecutors& executors, std::size_t chunk_size, std::size_t read_concurrency)
	: pool_(pool), executors_(executors), chunk_size_(chunk_size), read_concurrency_(std::max<std::size_t>(1, read_concurrency)) {}

void TarPacker::pack(BoundedQueue<FileMeta>& in_meta, BoundedQueue<DataChunk>& out_tar) {
	FileReader reader(pool_, executors_);
	TarWriteContext context{&out_tar, nullptr, &pool_, 0};

	auto* writer = archive_write_new();
	if (writer == nullptr) {
		throw std::runtime_error("failed to allocate libarchive writer");
	}

	try {
		if (archive_write_set_format_pax_restricted(writer) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to configure tar writer");
		}
		if (archive_write_open(writer, &context, nullptr, &TarPacker::archive_write_callback, &TarPacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to open tar stream");
		}

		while (auto meta = in_meta.pop()) {
			add_entry(writer, *meta, reader);
		}

		if (archive_write_close(writer) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to finalize tar stream");
		}
		archive_write_free(writer);
		out_tar.close();
	} catch (...) {
		archive_write_free(writer);
		out_tar.close();
		throw;
	}
}

void TarPacker::pack(BoundedQueue<FileMeta>& in_meta, ConcurrentDataChunkChannel& out_tar) {
	FileReader reader(pool_, executors_);
	TarWriteContext context{nullptr, &out_tar, &pool_, 0};

	auto* writer = archive_write_new();
	if (writer == nullptr) {
		throw std::runtime_error("failed to allocate libarchive writer");
	}

	try {
		if (archive_write_set_format_pax_restricted(writer) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to configure tar writer");
		}
		if (archive_write_open(writer, &context, nullptr, &TarPacker::archive_write_callback, &TarPacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to open tar stream");
		}

		while (auto meta = in_meta.pop()) {
			add_entry(writer, *meta, reader);
		}

		if (archive_write_close(writer) != ARCHIVE_OK) {
			throw_archive_error(writer, "failed to finalize tar stream");
		}
		archive_write_free(writer);
		out_tar.close();
	} catch (...) {
		archive_write_free(writer);
		out_tar.close();
		throw;
	}
}

la_ssize_t TarPacker::archive_write_callback(struct archive*, void* client_data, const void* buffer, size_t length) {
	auto* context = static_cast<TarWriteContext*>(client_data);
	auto owned_buffer = context->pool->acquire(length);
	std::memcpy(owned_buffer.get(), buffer, length);
	if (context->out_channel != nullptr) {
		send_chunk_blocking(*context->out_channel, DataChunk{std::move(owned_buffer), length, context->offset, false});
	} else {
		context->out_tar->push(DataChunk{std::move(owned_buffer), length, context->offset, false});
	}
	context->offset += length;
	return static_cast<la_ssize_t>(length);
}

int TarPacker::archive_close_callback(struct archive*, void*) {
	return ARCHIVE_OK;
}

void TarPacker::add_entry(struct archive* writer, const FileMeta& meta, const FileReader& reader) const {
	auto* entry = archive_entry_new();
	if (entry == nullptr) {
		throw std::runtime_error("failed to allocate archive entry");
	}

	archive_entry_set_pathname(entry, meta.relative_path_in_tar.c_str());
	archive_entry_set_perm(entry, permissions_to_mode(meta.status.permissions()));

	const auto status_type = meta.status.type();
	if (status_type == std::filesystem::file_type::directory || meta.relative_path_in_tar == ".") {
		archive_entry_set_filetype(entry, AE_IFDIR);
		archive_entry_set_size(entry, 0);
	} else if (status_type == std::filesystem::file_type::regular) {
		archive_entry_set_filetype(entry, AE_IFREG);
		archive_entry_set_size(entry, static_cast<la_int64_t>(meta.size));
	} else {
		archive_entry_free(entry);
		return;
	}

	if (archive_write_header(writer, entry) != ARCHIVE_OK) {
		auto message = std::string("failed to write tar header for ") + meta.relative_path_in_tar + ": " + archive_error_string(writer);
		archive_entry_free(entry);
		throw std::runtime_error(message);
	}

	if (status_type == std::filesystem::file_type::regular) {
		std::map<std::uint64_t, std::future<DataChunk>> pending_reads;
		std::uint64_t next_submit_offset = 0;
		std::uint64_t next_write_offset = 0;

		auto submit_read = [&](std::uint64_t offset) {
			const auto length = static_cast<std::size_t>(std::min<std::uintmax_t>(chunk_size_, meta.size - offset));
			pending_reads.emplace(offset, reader.read_chunk_async(meta.full_path, offset, length));
		};

		while (next_submit_offset < meta.size && pending_reads.size() < read_concurrency_) {
			submit_read(next_submit_offset);
			next_submit_offset += static_cast<std::uint64_t>(std::min<std::uintmax_t>(chunk_size_, meta.size - next_submit_offset));
		}

		while (!pending_reads.empty()) {
			auto pending = pending_reads.find(next_write_offset);
			if (pending == pending_reads.end()) {
				archive_entry_free(entry);
				throw std::runtime_error("reorder buffer lost file chunk ordering");
			}

			auto chunk = pending->second.get();
			pending_reads.erase(pending);
			if (chunk.length == 0) {
				archive_entry_free(entry);
				throw std::runtime_error("unexpected end of file while packing: " + meta.full_path.string());
			}

			const auto bytes_written = archive_write_data(writer, chunk.data.get(), chunk.length);
			if (bytes_written < 0 || static_cast<std::size_t>(bytes_written) != chunk.length) {
				archive_entry_free(entry);
				throw_archive_error(writer, "failed to write file payload into tar");
			}
			next_write_offset += chunk.length;

			while (next_submit_offset < meta.size && pending_reads.size() < read_concurrency_) {
				submit_read(next_submit_offset);
				next_submit_offset += static_cast<std::uint64_t>(std::min<std::uintmax_t>(chunk_size_, meta.size - next_submit_offset));
			}
		}
	}

	archive_entry_free(entry);
}

TarUnpacker::TarUnpacker(const std::filesystem::path& destination_root) : destination_root_(destination_root) {
	std::filesystem::create_directories(destination_root_);
}

void TarUnpacker::unpack(BoundedQueue<DataChunk>& in_tar) {
	TarReadContext context{&in_tar, nullptr, std::nullopt, 0};
	auto* reader = archive_read_new();
	auto* disk_writer = archive_write_disk_new();
	if (reader == nullptr) {
		throw std::runtime_error("failed to allocate libarchive reader");
	}
	if (disk_writer == nullptr) {
		archive_read_free(reader);
		throw std::runtime_error("failed to allocate libarchive disk writer");
	}

	try {
		if (archive_read_support_format_tar(reader) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to enable tar format support");
		}
		archive_write_disk_set_options(disk_writer, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM | ARCHIVE_EXTRACT_SECURE_NODOTDOT);
		archive_write_disk_set_standard_lookup(disk_writer);
		if (archive_read_open(reader, &context, nullptr, &TarUnpacker::archive_read_callback, &TarUnpacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to open tar input stream");
		}

		archive_entry* entry = nullptr;
		while (true) {
			const auto status = archive_read_next_header(reader, &entry);
			if (status == ARCHIVE_EOF) {
				break;
			}
			if (status != ARCHIVE_OK) {
				throw_archive_error(reader, "failed to read tar header");
			}

			const auto output_path = resolve_output_path(archive_entry_pathname(entry));
			archive_entry_set_pathname(entry, output_path.string().c_str());
			const auto write_status = archive_write_header(disk_writer, entry);
			if (write_status != ARCHIVE_OK) {
				throw std::runtime_error(std::string("failed to prepare extracted output: ") + archive_error_string(disk_writer));
			}

			const void* buffer = nullptr;
			size_t length = 0;
			la_int64_t offset = 0;
			while (true) {
				const auto block_status = archive_read_data_block(reader, &buffer, &length, &offset);
				if (block_status == ARCHIVE_EOF) {
					break;
				}
				if (block_status != ARCHIVE_OK) {
					throw_archive_error(reader, "failed to stream tar payload");
				}
				if (archive_write_data_block(disk_writer, buffer, length, offset) != ARCHIVE_OK) {
					throw std::runtime_error(std::string("failed to write extracted payload: ") + archive_error_string(disk_writer));
				}
			}

			if (archive_write_finish_entry(disk_writer) != ARCHIVE_OK) {
				throw std::runtime_error(std::string("failed to finalize extracted entry: ") + archive_error_string(disk_writer));
			}
		}

		archive_read_close(reader);
		archive_write_close(disk_writer);
		archive_read_free(reader);
		archive_write_free(disk_writer);
	} catch (...) {
		archive_read_free(reader);
		archive_write_free(disk_writer);
		throw;
	}
}

void TarUnpacker::unpack(ConcurrentDataChunkChannel& in_tar) {
	TarReadContext context{nullptr, &in_tar, std::nullopt, 0};
	auto* reader = archive_read_new();
	auto* disk_writer = archive_write_disk_new();
	if (reader == nullptr) {
		throw std::runtime_error("failed to allocate libarchive reader");
	}
	if (disk_writer == nullptr) {
		archive_read_free(reader);
		throw std::runtime_error("failed to allocate libarchive disk writer");
	}

	try {
		if (archive_read_support_format_tar(reader) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to enable tar format support");
		}
		archive_write_disk_set_options(disk_writer, ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM | ARCHIVE_EXTRACT_SECURE_NODOTDOT);
		archive_write_disk_set_standard_lookup(disk_writer);
		if (archive_read_open(reader, &context, nullptr, &TarUnpacker::archive_read_callback, &TarUnpacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to open tar input stream");
		}

		archive_entry* entry = nullptr;
		while (true) {
			const auto status = archive_read_next_header(reader, &entry);
			if (status == ARCHIVE_EOF) {
				break;
			}
			if (status != ARCHIVE_OK) {
				throw_archive_error(reader, "failed to read tar header");
			}

			const auto output_path = resolve_output_path(archive_entry_pathname(entry));
			archive_entry_set_pathname(entry, output_path.string().c_str());
			const auto write_status = archive_write_header(disk_writer, entry);
			if (write_status != ARCHIVE_OK) {
				throw std::runtime_error(std::string("failed to prepare extracted output: ") + archive_error_string(disk_writer));
			}

			const void* buffer = nullptr;
			size_t length = 0;
			la_int64_t offset = 0;
			while (true) {
				const auto block_status = archive_read_data_block(reader, &buffer, &length, &offset);
				if (block_status == ARCHIVE_EOF) {
					break;
				}
				if (block_status != ARCHIVE_OK) {
					throw_archive_error(reader, "failed to stream tar payload");
				}
				if (archive_write_data_block(disk_writer, buffer, length, offset) != ARCHIVE_OK) {
					throw std::runtime_error(std::string("failed to write extracted payload: ") + archive_error_string(disk_writer));
				}
			}

			if (archive_write_finish_entry(disk_writer) != ARCHIVE_OK) {
				throw std::runtime_error(std::string("failed to finalize extracted entry: ") + archive_error_string(disk_writer));
			}
		}

		archive_read_close(reader);
		archive_write_close(disk_writer);
		archive_read_free(reader);
		archive_write_free(disk_writer);
	} catch (...) {
		archive_read_free(reader);
		archive_write_free(disk_writer);
		throw;
	}
}

la_ssize_t TarUnpacker::archive_read_callback(struct archive*, void* client_data, const void** buffer) {
	auto* context = static_cast<TarReadContext*>(client_data);
	if (context->in_channel != nullptr) {
		context->current_chunk = receive_chunk_blocking(*context->in_channel);
		if (!context->current_chunk.has_value()) {
			*buffer = nullptr;
			return 0;
		}
		*buffer = context->current_chunk->data.get();
		context->offset += context->current_chunk->length;
		return static_cast<la_ssize_t>(context->current_chunk->length);
	}

	context->current_chunk = context->in_tar->pop();
	if (!context->current_chunk.has_value()) {
		*buffer = nullptr;
		return 0;
	}
	*buffer = context->current_chunk->data.get();
	context->offset += context->current_chunk->length;
	return static_cast<la_ssize_t>(context->current_chunk->length);
}

int TarUnpacker::archive_close_callback(struct archive*, void*) {
	return ARCHIVE_OK;
}

std::filesystem::path TarUnpacker::resolve_output_path(const char* raw_path) const {
	auto relative_path = normalize_relative_path(std::filesystem::path(raw_path));
	if (relative_path == ".") {
		return destination_root_;
	}
	return destination_root_ / relative_path;
}

} // namespace soratransport