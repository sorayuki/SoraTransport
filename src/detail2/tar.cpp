#include "tar.hpp"

#include "../detail/win32_util.hpp"

#include <cstring>
#include <stdexcept>

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

} // namespace soratransport::detail2