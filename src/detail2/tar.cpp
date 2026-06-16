#include "tar.hpp"

#include "../detail/win32_util.hpp"

#include <cstring>
#include <stdexcept>
#include <vector>

namespace soratransport::detail2 {

namespace {

struct TarWriteContext {
	BoundedQueue<DataChunk>* out_tar = nullptr;
	BufferPool* pool = nullptr;
	SemaphoreCor* output_budget = nullptr;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
	const CancelEvent* cancel_event = nullptr;
};

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
}

[[noreturn]] void throw_archive_error(struct archive* handle, const std::string& message) {
	const auto* detail = archive_error_string(handle);
	if (detail == nullptr || std::char_traits<char>::length(detail) == 0) {
		throw std::runtime_error(message);
	}
	throw std::runtime_error(message + ": " + detail);
}

void apply_optional_timestamp(struct archive_entry* entry, const std::optional<FileTimestamp>& timestamp, int which) {
	if (!timestamp.has_value()) {
		return;
	}
	if (which == 0) {
		archive_entry_set_birthtime(entry, timestamp->seconds, timestamp->nanoseconds);
	} else if (which == 1) {
		archive_entry_set_atime(entry, timestamp->seconds, timestamp->nanoseconds);
	} else if (which == 2) {
		archive_entry_set_mtime(entry, timestamp->seconds, timestamp->nanoseconds);
	} else {
		archive_entry_set_ctime(entry, timestamp->seconds, timestamp->nanoseconds);
	}
}

void apply_entry_metadata(struct archive_entry* entry, const FileMeta& meta) {
	archive_entry_set_pathname_utf8(entry, meta.relative_path_in_tar.c_str());
	apply_optional_timestamp(entry, meta.creation_time, 0);
	apply_optional_timestamp(entry, meta.last_access_time, 1);
	apply_optional_timestamp(entry, meta.last_write_time, 2);
	apply_optional_timestamp(entry, meta.change_time, 3);
	if (meta.windows_file_attributes.has_value()) {
		archive_entry_set_fflags(entry, static_cast<unsigned long>(*meta.windows_file_attributes), 0);
	}
	archive_entry_set_perm(entry, 0644);
	archive_entry_set_uid(entry, 0);
	archive_entry_set_gid(entry, 0);
	archive_entry_set_uname(entry, "user");
	archive_entry_set_gname(entry, "group");
}

} // namespace

TarPacker::TarPacker(BufferPool& pool, std::size_t chunk_size, SemaphoreCor* output_budget)
	: pool_(pool),
	  chunk_size_(chunk_size),
	  output_budget_(output_budget) {}

void TarPacker::pack(
	BoundedQueue<OpenedFile>& in_files,
	BoundedQueue<DataChunk>& out_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	TarWriteContext context{&out_tar, &pool_, output_budget_, 0, uncompressed_bytes_counter, cancel_event};
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

		while (auto opened_file = in_files.pop()) {
			throw_if_cancelled(cancel_event);
			opened_file->prefetch_guard.reset();
			opened_file->prefetched_bytes = 0;
			add_entry(writer, *opened_file, file_counter, cancel_event);
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
	throw_if_cancelled(context->cancel_event);
	auto owned_buffer = context->pool->acquire(length);
	std::memcpy(owned_buffer.get(), buffer, length);
	auto budget_guard = context->output_budget == nullptr
		? SemaphoreCor::Guard{}
		: context->output_budget->acquire_blocking(length);
	context->out_tar->push(make_budgeted_chunk(std::move(owned_buffer), length, context->offset, std::move(budget_guard)));
	if (context->uncompressed_bytes_counter != nullptr) {
		context->uncompressed_bytes_counter->fetch_add(length, std::memory_order_relaxed);
	}
	context->offset += length;
	return static_cast<la_ssize_t>(length);
}

int TarPacker::archive_close_callback(struct archive*, void*) {
	return ARCHIVE_OK;
}

void TarPacker::add_entry(struct archive* writer, OpenedFile& opened_file, std::atomic<std::uint64_t>* file_counter, const CancelEvent* cancel_event) const {
	auto& meta = opened_file.meta;
	throw_if_cancelled(cancel_event);
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
			throw_if_cancelled(cancel_event);
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

// ---------------------------------------------------------------------------
// TarUnpacker (native detail2 implementation using BufferedFileWriter)
// ---------------------------------------------------------------------------

namespace {

struct RestorableMetadata {
	std::filesystem::path output_path;
	std::uint64_t file_size = 0;
	std::filesystem::perms permissions = std::filesystem::perms::unknown;
	std::optional<FileTimestamp> creation_time;
	std::optional<FileTimestamp> last_access_time;
	std::optional<FileTimestamp> last_write_time;
};

std::optional<FileTimestamp> to_file_timestamp(time_t sec, long nsec) {
	if (sec == 0 && nsec == 0) {
		return std::nullopt;
	}
	return FileTimestamp{sec, static_cast<long>(nsec)};
}

FILETIME timestamp_to_filetime(const FileTimestamp& ts) {
	ULARGE_INTEGER uli;
	uli.QuadPart = (static_cast<std::uint64_t>(ts.seconds) + 11644473600ULL) * 10000000ULL
		+ static_cast<std::uint64_t>(ts.nanoseconds) / 100ULL;
	FILETIME ft;
	ft.dwLowDateTime = uli.LowPart;
	ft.dwHighDateTime = uli.HighPart;
	return ft;
}

RestorableMetadata capture_restorable_metadata(archive_entry* entry, const std::filesystem::path& output_path) {
	RestorableMetadata m;
	m.output_path = output_path;
	m.file_size = static_cast<std::uint64_t>(std::max<la_int64_t>(0, archive_entry_size(entry)));
	m.permissions = static_cast<std::filesystem::perms>(archive_entry_perm(entry) & 0777);
	m.creation_time = to_file_timestamp(archive_entry_birthtime(entry), archive_entry_birthtime_nsec(entry));
	m.last_access_time = to_file_timestamp(archive_entry_atime(entry), archive_entry_atime_nsec(entry));
	m.last_write_time = to_file_timestamp(archive_entry_mtime(entry), archive_entry_mtime_nsec(entry));
	return m;
}

void apply_permissions_if_possible(const RestorableMetadata& metadata) {
	if (metadata.permissions == std::filesystem::perms::unknown) {
		return;
	}
	std::error_code ec;
	std::filesystem::permissions(metadata.output_path, metadata.permissions,
		std::filesystem::perm_options::replace, ec);
	if (ec && ec.value() != static_cast<int>(std::errc::operation_not_supported)
		&& ec.value() != static_cast<int>(std::errc::function_not_supported)) {
		throw std::runtime_error("failed to set permissions: " + path_to_utf8_string(metadata.output_path) + ": " + ec.message());
	}
}

void apply_timestamps(const RestorableMetadata& metadata, bool is_directory) {
	if (!metadata.creation_time && !metadata.last_access_time && !metadata.last_write_time) {
		return;
	}
	auto display = path_to_utf8_string(metadata.output_path);
	UniqueWin32Handle handle(::CreateFileW(
		metadata.output_path.c_str(),
		FILE_WRITE_ATTRIBUTES,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		is_directory ? (FILE_FLAG_BACKUP_SEMANTICS | FILE_ATTRIBUTE_NORMAL) : FILE_ATTRIBUTE_NORMAL,
		nullptr));
	if (!handle.valid()) {
		throw make_win32_error("failed to open for timestamp restore: " + display);
	}
	FILETIME ct{}, lat{}, lwt{};
	FILETIME* pct = nullptr;
	FILETIME* plat = nullptr;
	FILETIME* plwt = nullptr;
	if (metadata.creation_time) {
		ct = timestamp_to_filetime(*metadata.creation_time);
		pct = &ct;
	}
	if (metadata.last_access_time) {
		lat = timestamp_to_filetime(*metadata.last_access_time);
		plat = &lat;
	}
	if (metadata.last_write_time) {
		lwt = timestamp_to_filetime(*metadata.last_write_time);
		plwt = &lwt;
	}
	if (!::SetFileTime(handle.get(), pct, plat, plwt)) {
		throw make_win32_error("failed to restore timestamps: " + display);
	}
}

void apply_restorable_metadata(const RestorableMetadata& metadata, bool is_directory, const CancelEvent* cancel_event) {
	throw_if_cancelled(cancel_event);
	apply_timestamps(metadata, is_directory);
	throw_if_cancelled(cancel_event);
	apply_permissions_if_possible(metadata);
}

struct TarReadContext {
	BoundedQueue<DataChunk>* in_tar = nullptr;
	std::optional<DataChunk> current_chunk;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
	const CancelEvent* cancel_event = nullptr;
};

void ensure_directory_exists(const std::filesystem::path& path) {
	std::error_code ec;
	if (std::filesystem::exists(path, ec)) {
		return;
	}
	std::filesystem::create_directories(path, ec);
	if (ec) {
		throw std::runtime_error("failed to create directory: " + path_to_utf8_string(path) + ": " + ec.message());
	}
}

} // namespace

TarUnpacker::TarUnpacker(
	const std::filesystem::path& destination_root,
	BufferPool& pool,
	TaskExecutor& executor,
	PipelineTuning tuning,
	CancelEvent* cancel_event)
	: destination_root_(destination_root),
	  pool_(pool),
	  executor_(executor),
	  tuning_(std::move(tuning)),
	  cancel_event_(cancel_event) {
	ensure_directory_exists(destination_root_);
}

void TarUnpacker::unpack(
	BoundedQueue<DataChunk>& in_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	auto* effective_cancel = cancel_event != nullptr ? cancel_event : cancel_event_;
	TarReadContext context{&in_tar, std::nullopt, 0, uncompressed_bytes_counter, effective_cancel};

	auto* reader = archive_read_new();
	if (reader == nullptr) {
		throw std::runtime_error("failed to allocate libarchive reader");
	}

	try {
		if (archive_read_support_format_tar(reader) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to enable tar format support");
		}
		if (archive_read_open(reader, &context, nullptr, &TarUnpacker::archive_read_callback, &TarUnpacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to open tar input stream");
		}

		archive_entry* entry = nullptr;
		while (true) {
			throw_if_cancelled(effective_cancel);
			const auto status = archive_read_next_header(reader, &entry);
			if (status == ARCHIVE_EOF) {
				break;
			}
			if (status != ARCHIVE_OK) {
				throw_archive_error(reader, "failed to read tar header");
			}

			const auto file_type = archive_entry_filetype(entry);
			if (file_type == AE_IFLNK) {
				archive_read_data_skip(reader);
				continue;
			}

			const auto* utf8_name = archive_entry_pathname_utf8(entry);
			if (utf8_name == nullptr) {
				throw std::runtime_error("archive entry has non-UTF-8 pathname");
			}
			const auto output_path = resolve_output_path(utf8_name);
			auto metadata = capture_restorable_metadata(entry, output_path);

			if (file_type == AE_IFDIR) {
				ensure_directory_exists(metadata.output_path);
				apply_restorable_metadata(metadata, true, effective_cancel);
				continue;
			}
			if (file_type != AE_IFREG) {
				archive_read_data_skip(reader);
				continue;
			}

			// Buffer all chunks for this file, then write via BufferedFileWriter
			struct FileChunk {
				std::shared_ptr<std::uint8_t> data;
				std::size_t length = 0;
				std::uint64_t offset = 0;
			};
			std::vector<FileChunk> chunks;
			const void* buffer = nullptr;
			size_t length = 0;
			la_int64_t data_offset = 0;
			while (true) {
				throw_if_cancelled(effective_cancel);
				const auto block_status = archive_read_data_block(reader, &buffer, &length, &data_offset);
				if (block_status == ARCHIVE_EOF) {
					break;
				}
				if (block_status != ARCHIVE_OK) {
					throw_archive_error(reader, "failed to stream tar payload");
				}
				auto owned = pool_.acquire(length);
				std::memcpy(owned.get(), buffer, length);
				chunks.push_back({std::move(owned), length, static_cast<std::uint64_t>(data_offset)});
			}

			ensure_directory_exists(metadata.output_path.parent_path());

			auto writer = BufferedFileWriter::create(
				metadata.output_path,
				executor_,
				tuning_,
				metadata.file_size,
				effective_cancel);

			for (const auto& chunk : chunks) {
				throw_if_cancelled(effective_cancel);
				writer->write(std::span(chunk.data.get(), chunk.length));
			}
			writer->close();

			apply_restorable_metadata(metadata, false, effective_cancel);
			if (file_counter != nullptr) {
				file_counter->fetch_add(1, std::memory_order_relaxed);
			}
		}

		archive_read_close(reader);
		archive_read_free(reader);
	} catch (...) {
		archive_read_free(reader);
		throw;
	}
}

la_ssize_t TarUnpacker::archive_read_callback(struct archive*, void* client_data, const void** buffer) {
	auto* context = static_cast<TarReadContext*>(client_data);
	throw_if_cancelled(context->cancel_event);
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

std::filesystem::path TarUnpacker::resolve_output_path(const char* utf8_path) const {
	auto relative = std::filesystem::path(reinterpret_cast<const char8_t*>(utf8_path)).lexically_normal();
	if (relative.empty() || relative == ".") {
		return destination_root_;
	}
	if (relative.is_absolute()) {
		relative = relative.lexically_relative("/");
	}
	return destination_root_ / relative;
}

} // namespace soratransport::detail2