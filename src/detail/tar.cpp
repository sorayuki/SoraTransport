#include "pipeline.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <stdexcept>

namespace soratransport {

namespace {

struct TarWriteContext {
	BoundedQueue<DataChunk>* out_tar = nullptr;
	BufferPool* pool = nullptr;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
};

struct TarReadContext {
	BoundedQueue<DataChunk>* in_tar = nullptr;
	std::optional<DataChunk> current_chunk;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
};

int permissions_to_mode(std::filesystem::perms permissions) {
	return static_cast<int>(permissions) & 0777;
}

void set_entry_timestamp(
	archive_entry* entry,
	const std::optional<FileTimestamp>& timestamp,
	void (*setter)(archive_entry*, time_t, long)) {
	if (!timestamp.has_value()) {
		return;
	}
	setter(entry, static_cast<time_t>(timestamp->seconds), timestamp->nanoseconds);
}

void add_windows_attribute_metadata(archive_entry* entry, const FileMeta& meta) {
	if (!meta.windows_file_attributes.has_value()) {
		return;
	}
	const auto value = std::to_string(*meta.windows_file_attributes);
	archive_entry_xattr_add_entry(
		entry,
		"user.soratransport.win32_file_attributes",
		value.data(),
		value.size());
}

void apply_entry_metadata(archive_entry* entry, const FileMeta& meta) {
	archive_entry_set_pathname(entry, meta.relative_path_in_tar.c_str());
	archive_entry_set_perm(entry, permissions_to_mode(meta.status.permissions()));
	set_entry_timestamp(entry, meta.creation_time, &archive_entry_set_birthtime);
	set_entry_timestamp(entry, meta.last_access_time, &archive_entry_set_atime);
	set_entry_timestamp(entry, meta.last_write_time, &archive_entry_set_mtime);
	set_entry_timestamp(entry, meta.change_time, &archive_entry_set_ctime);
	add_windows_attribute_metadata(entry, meta);
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

} // namespace

TarPacker::TarPacker(BufferPool& pool, std::size_t chunk_size)
	: pool_(pool), chunk_size_(chunk_size) {}

void TarPacker::pack(
	BoundedQueue<OpenedFileReader>& in_meta,
	BoundedQueue<DataChunk>& out_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter) {
	TarWriteContext context{&out_tar, &pool_, 0, uncompressed_bytes_counter};

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
			meta->read_budget_lease.reset();
			add_entry(writer, *meta, file_counter);
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
	context->out_tar->push(DataChunk{std::move(owned_buffer), length, context->offset, false});
	if (context->uncompressed_bytes_counter != nullptr) {
		context->uncompressed_bytes_counter->fetch_add(length, std::memory_order_relaxed);
	}
	context->offset += length;
	return static_cast<la_ssize_t>(length);
}

int TarPacker::archive_close_callback(struct archive*, void*) {
	return ARCHIVE_OK;
}


void TarPacker::add_entry(struct archive* writer, OpenedFileReader& opened_file, std::atomic<std::uint64_t>* file_counter) const {
	auto& meta = opened_file.meta;
	auto* entry = archive_entry_new();
	if (entry == nullptr) {
		throw std::runtime_error("failed to allocate archive entry");
	}

	apply_entry_metadata(entry, meta);

	auto status_type = meta.status.type();
	if (status_type == std::filesystem::file_type::directory || meta.relative_path_in_tar == ".") {
		archive_entry_set_filetype(entry, AE_IFDIR);
		archive_entry_set_size(entry, 0);
	} else if (status_type == std::filesystem::file_type::regular) {
		archive_entry_set_filetype(entry, AE_IFREG);
		archive_entry_set_size(entry, meta.size);
		if (file_counter != nullptr) {
			file_counter->fetch_add(1, std::memory_order_relaxed);
		}
	} else if (status_type == std::filesystem::file_type::symlink) {
		if (!meta.symlink_target.has_value()) {
			archive_entry_free(entry);
			throw std::runtime_error("failed to resolve symlink target for tar entry: " + meta.relative_path_in_tar);
		}
		archive_entry_set_filetype(entry, AE_IFLNK);
		archive_entry_set_size(entry, 0);
		archive_entry_set_symlink(entry, meta.symlink_target->c_str());
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
		if (!opened_file.reader.has_value() || !opened_file.reader->is_open()) {
			archive_entry_free(entry);
			throw std::runtime_error("file reader was not opened before pack stage");
		}

		auto& reader = *opened_file.reader;
		while (!reader.eof()) {
			auto chunk = reader.read_next_chunk();
			if (chunk.length == 0) {
				archive_entry_free(entry);
				throw std::runtime_error("unexpected end of file while packing");
			}

			auto bytes_written = archive_write_data(writer, chunk.data.get(), chunk.length);
			if (bytes_written < 0 || bytes_written != chunk.length) {
				archive_entry_free(entry);
				throw_archive_error(writer, "failed to write file payload into tar");
			}
		}
	}

	archive_entry_free(entry);
}

TarUnpacker::TarUnpacker(const std::filesystem::path& destination_root) : destination_root_(destination_root) {
	std::filesystem::create_directories(destination_root_);
}

void TarUnpacker::unpack(
	BoundedQueue<DataChunk>& in_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter) {
	TarReadContext context{&in_tar, std::nullopt, 0, uncompressed_bytes_counter};
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
			if (file_counter != nullptr && archive_entry_filetype(entry) == AE_IFREG) {
				file_counter->fetch_add(1, std::memory_order_relaxed);
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
	context->current_chunk = context->in_tar->pop();
	if (!context->current_chunk.has_value()) {
		*buffer = nullptr;
		return 0;
	}
	*buffer = context->current_chunk->data.get();
	if (context->uncompressed_bytes_counter != nullptr) {
		context->uncompressed_bytes_counter->fetch_add(context->current_chunk->length, std::memory_order_relaxed);
	}
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
