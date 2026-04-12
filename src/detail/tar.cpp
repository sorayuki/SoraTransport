#include "pipeline.hpp"
#include "win32_util.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <list>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

#include <windows.h>

namespace soratransport {

namespace {

struct TarWriteContext {
	BoundedQueue<DataChunk>* out_tar = nullptr;
	BufferPool* pool = nullptr;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
	const CancelEvent* cancel_event = nullptr;
};

struct TarReadContext {
	BoundedQueue<DataChunk>* in_tar = nullptr;
	std::optional<DataChunk> current_chunk;
	std::uint64_t offset = 0;
	std::atomic<std::uint64_t>* uncompressed_bytes_counter = nullptr;
	const CancelEvent* cancel_event = nullptr;
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
	archive_entry_set_pathname_utf8(entry, meta.relative_path_in_tar.c_str());
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

void throw_if_cancelled(const CancelEvent* cancel_event) {
	if (cancel_event != nullptr && cancel_event->is_cancelled()) {
		throw CancelledError();
	}
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

constexpr std::int64_t kWindowsToUnixEpochOffset100ns = 116444736000000000ll;
constexpr std::size_t kBufferedExtractWriteBatchSize = 4 * 1024 * 1024;
constexpr auto kBufferedExtractWriteMaxDelay = std::chrono::milliseconds(100);

struct UniqueWin32Handle {
	explicit UniqueWin32Handle(HANDLE input_handle = INVALID_HANDLE_VALUE) noexcept : handle(input_handle) {}
	~UniqueWin32Handle() {
		reset();
	}

	UniqueWin32Handle(const UniqueWin32Handle&) = delete;
	UniqueWin32Handle& operator=(const UniqueWin32Handle&) = delete;

	UniqueWin32Handle(UniqueWin32Handle&& other) noexcept : handle(other.release()) {}

	UniqueWin32Handle& operator=(UniqueWin32Handle&& other) noexcept {
		if (this != &other) {
			reset(other.release());
		}
		return *this;
	}

	void reset(HANDLE new_handle = INVALID_HANDLE_VALUE) noexcept {
		if (handle != INVALID_HANDLE_VALUE) {
			::CloseHandle(handle);
		}
		handle = new_handle;
	}

	HANDLE get() const noexcept {
		return handle;
	}

	HANDLE release() noexcept {
		const auto released = handle;
		handle = INVALID_HANDLE_VALUE;
		return released;
	}

	bool valid() const noexcept {
		return handle != INVALID_HANDLE_VALUE;
	}

	HANDLE handle = INVALID_HANDLE_VALUE;
};

void flush_file_buffers_if_supported(HANDLE handle, const std::string& path, std::string_view action) {
	if (::FlushFileBuffers(handle)) {
		return;
	}

	const auto error = ::GetLastError();
	if (error == ERROR_INVALID_FUNCTION && ::GetFileType(handle) != FILE_TYPE_DISK) {
		return;
	}

	throw make_win32_error(std::string(action) + ": " + path, error);
}

FILETIME timestamp_to_filetime(const FileTimestamp& timestamp) {
	const auto total_100ns = (timestamp.seconds * 10000000ll)
		+ static_cast<std::int64_t>(timestamp.nanoseconds / 100ll)
		+ kWindowsToUnixEpochOffset100ns;
	ULARGE_INTEGER value{};
	value.QuadPart = static_cast<ULONGLONG>(std::max<std::int64_t>(0, total_100ns));
	FILETIME result{};
	result.dwLowDateTime = value.LowPart;
	result.dwHighDateTime = value.HighPart;
	return result;
}

std::optional<FileTimestamp> archive_entry_birthtime_value(archive_entry* entry) {
	if (!archive_entry_birthtime_is_set(entry)) {
		return std::nullopt;
	}
	return FileTimestamp{
		static_cast<std::int64_t>(archive_entry_birthtime(entry)),
		archive_entry_birthtime_nsec(entry),
	};
}

std::optional<FileTimestamp> archive_entry_atime_value(archive_entry* entry) {
	if (!archive_entry_atime_is_set(entry)) {
		return std::nullopt;
	}
	return FileTimestamp{
		static_cast<std::int64_t>(archive_entry_atime(entry)),
		archive_entry_atime_nsec(entry),
	};
}

std::optional<FileTimestamp> archive_entry_mtime_value(archive_entry* entry) {
	if (!archive_entry_mtime_is_set(entry)) {
		return std::nullopt;
	}
	return FileTimestamp{
		static_cast<std::int64_t>(archive_entry_mtime(entry)),
		archive_entry_mtime_nsec(entry),
	};
}

struct RestorableMetadata {
	std::filesystem::path output_path;
	std::uint64_t file_size = 0;
	std::filesystem::perms permissions = std::filesystem::perms::unknown;
	std::optional<FileTimestamp> creation_time;
	std::optional<FileTimestamp> last_access_time;
	std::optional<FileTimestamp> last_write_time;
};

RestorableMetadata capture_restorable_metadata(archive_entry* entry, const std::filesystem::path& output_path) {
	RestorableMetadata metadata;
	metadata.output_path = output_path;
	metadata.file_size = static_cast<std::uint64_t>(std::max<la_int64_t>(0, archive_entry_size(entry)));
	metadata.permissions = static_cast<std::filesystem::perms>(archive_entry_perm(entry) & 0777);
	metadata.creation_time = archive_entry_birthtime_value(entry);
	metadata.last_access_time = archive_entry_atime_value(entry);
	metadata.last_write_time = archive_entry_mtime_value(entry);
	return metadata;
}

void apply_permissions_if_possible(const RestorableMetadata& metadata) {
	if (metadata.permissions == std::filesystem::perms::unknown) {
		return;
	}

	std::error_code permissions_error;
	std::filesystem::permissions(
		metadata.output_path,
		metadata.permissions,
		std::filesystem::perm_options::replace,
		permissions_error);
	if (permissions_error
		&& permissions_error.value() != static_cast<int>(std::errc::operation_not_supported)
		&& permissions_error.value() != static_cast<int>(std::errc::function_not_supported)) {
		throw std::runtime_error(
			"failed to set extracted permissions: "
			+ path_to_utf8_string(metadata.output_path)
			+ ": "
			+ permissions_error.message());
	}
}

void apply_timestamps_to_handle_if_present(HANDLE handle, const RestorableMetadata& metadata, const std::string& display_path) {
	if (!metadata.creation_time.has_value()
		&& !metadata.last_access_time.has_value()
		&& !metadata.last_write_time.has_value()) {
		return;
	}

	FILETIME creation_time{};
	FILETIME last_access_time{};
	FILETIME last_write_time{};
	FILETIME* creation_ptr = nullptr;
	FILETIME* access_ptr = nullptr;
	FILETIME* write_ptr = nullptr;

	if (metadata.creation_time.has_value()) {
		creation_time = timestamp_to_filetime(*metadata.creation_time);
		creation_ptr = &creation_time;
	}
	if (metadata.last_access_time.has_value()) {
		last_access_time = timestamp_to_filetime(*metadata.last_access_time);
		access_ptr = &last_access_time;
	}
	if (metadata.last_write_time.has_value()) {
		last_write_time = timestamp_to_filetime(*metadata.last_write_time);
		write_ptr = &last_write_time;
	}

	if (!::SetFileTime(handle, creation_ptr, access_ptr, write_ptr)) {
		throw make_win32_error("failed to restore extracted timestamps: " + display_path);
	}
}

void apply_timestamps_if_present(const RestorableMetadata& metadata, bool is_directory) {
	if (!metadata.creation_time.has_value()
		&& !metadata.last_access_time.has_value()
		&& !metadata.last_write_time.has_value()) {
		return;
	}

	const auto display_path = path_to_utf8_string(metadata.output_path);
	const auto flags = FILE_FLAG_BACKUP_SEMANTICS | FILE_ATTRIBUTE_NORMAL;
	UniqueWin32Handle handle(::CreateFileW(
		metadata.output_path.c_str(),
		FILE_WRITE_ATTRIBUTES,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		nullptr,
		OPEN_EXISTING,
		is_directory ? flags : FILE_ATTRIBUTE_NORMAL,
		nullptr));
	if (!handle.valid()) {
		throw make_win32_error("failed to open extracted path for timestamp restore: " + display_path);
	}

	apply_timestamps_to_handle_if_present(handle.get(), metadata, display_path);
}

void apply_restorable_metadata(const RestorableMetadata& metadata, bool is_directory, const CancelEvent* cancel_event) {
	throw_if_cancelled(cancel_event);
	apply_timestamps_if_present(metadata, is_directory);
	throw_if_cancelled(cancel_event);
	apply_permissions_if_possible(metadata);
}

std::size_t path_depth(const std::filesystem::path& path) {
	return static_cast<std::size_t>(std::distance(path.begin(), path.end()));
}

struct ExtractWriteChunk : IQueueDisposable {
	void Dispose() noexcept override {
		write_budget_lease.reset();
		chunk.Dispose();
	}

	DataChunk chunk;
	WriteBudgetLease write_budget_lease;
};

class CreatedDirectoryIndex {
public:
	explicit CreatedDirectoryIndex(const CancelEvent* cancel_event) : cancel_event_(cancel_event) {}

	void mark_created(const std::filesystem::path& path) {
		if (path.empty()) {
			return;
		}

		auto entry = std::make_shared<EntryState>();
		entry->state = DirectoryState::Created;
		std::lock_guard lock(mutex_);
		entries_[normalize_key(path)] = std::move(entry);
	}

	void ensure_directory_exists(const std::filesystem::path& path) {
		if (path.empty()) {
			return;
		}

		throw_if_cancelled(cancel_event_);
		const auto normalized_path = std::filesystem::absolute(path).lexically_normal();
		const auto key = normalized_path.wstring();

		std::shared_ptr<EntryState> entry;
		bool should_create = false;
		{
			std::lock_guard lock(mutex_);
			auto it = entries_.find(key);
			if (it == entries_.end()) {
				entry = std::make_shared<EntryState>();
				entries_.emplace(key, entry);
				should_create = true;
			} else {
				entry = it->second;
			}
		}

		if (should_create) {
			try {
				throw_if_cancelled(cancel_event_);
				std::filesystem::create_directories(normalized_path);
				{
					std::lock_guard entry_lock(entry->mutex);
					entry->state = DirectoryState::Created;
				}
				entry->cv.notify_all();
			} catch (...) {
				{
					std::lock_guard entry_lock(entry->mutex);
					entry->state = DirectoryState::Failed;
					entry->error = std::current_exception();
				}
				entry->cv.notify_all();
				throw;
			}
			return;
		}

		std::unique_lock entry_lock(entry->mutex);
		while (entry->state == DirectoryState::Creating) {
			throw_if_cancelled(cancel_event_);
			entry->cv.wait_for(entry_lock, std::chrono::milliseconds(50));
		}
		if (entry->state == DirectoryState::Failed) {
			std::rethrow_exception(entry->error);
		}
	}

private:
	enum class DirectoryState {
		Creating,
		Created,
		Failed,
	};

	struct EntryState {
		std::mutex mutex;
		std::condition_variable cv;
		DirectoryState state = DirectoryState::Creating;
		std::exception_ptr error;
	};

	std::wstring normalize_key(const std::filesystem::path& path) const {
		return std::filesystem::absolute(path).lexically_normal().wstring();
	}

	const CancelEvent* cancel_event_ = nullptr;
	std::mutex mutex_;
	std::unordered_map<std::wstring, std::shared_ptr<EntryState>> entries_;
};

class DirectoryMetadataFinalizer {
public:
	void record(RestorableMetadata metadata) {
		records_.push_back(std::move(metadata));
	}

	void apply_all(const CancelEvent* cancel_event) {
		std::sort(records_.begin(), records_.end(), [](const RestorableMetadata& left, const RestorableMetadata& right) {
			const auto left_depth = path_depth(left.output_path);
			const auto right_depth = path_depth(right.output_path);
			if (left_depth != right_depth) {
				return left_depth > right_depth;
			}
			return left.output_path.native() < right.output_path.native();
		});

		for (const auto& record : records_) {
			apply_restorable_metadata(record, true, cancel_event);
		}
	}

private:
	std::vector<RestorableMetadata> records_;
};

void write_small_extracted_file(
	const RestorableMetadata& metadata,
	BoundedQueue<ExtractWriteChunk>& queue,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	const auto display_path = path_to_utf8_string(metadata.output_path);
	UniqueWin32Handle handle(::CreateFileW(
		metadata.output_path.c_str(),
		GENERIC_WRITE,
		0,
		nullptr,
		CREATE_ALWAYS,
		FILE_ATTRIBUTE_NORMAL,
		nullptr));
	if (!handle.valid()) {
		throw make_win32_error("failed to open extracted output file: " + display_path);
	}

	auto buffered_output = make_heap_buffer(kBufferedExtractWriteBatchSize);
	std::vector<WriteBudgetLease> buffered_leases;
	std::size_t buffered_size = 0;
	std::uint64_t buffered_offset = 0;
	std::uint64_t committed_offset = 0;
	std::uint64_t expected_offset = 0;
	std::optional<std::chrono::steady_clock::time_point> fill_started_at;

	auto flush_buffer = [&] {
		if (buffered_size == 0) {
			return;
		}
		if (buffered_offset != committed_offset) {
			throw std::runtime_error("small extracted file buffered write became non-sequential: " + display_path);
		}
		DWORD bytes_written = 0;
		if (!::WriteFile(
				handle.get(),
				buffered_output.get(),
				static_cast<DWORD>(buffered_size),
				&bytes_written,
				nullptr)) {
			throw make_win32_error("failed to write extracted output file: " + display_path);
		}
		if (bytes_written != buffered_size) {
			throw std::runtime_error("unexpected short write to extracted output file: " + display_path);
		}
		committed_offset += buffered_size;
		buffered_size = 0;
		buffered_offset = committed_offset;
		buffered_leases.clear();
		fill_started_at.reset();
	};

	while (auto chunk = queue.pop()) {
		throw_if_cancelled(cancel_event);
		if (chunk->chunk.offset != expected_offset) {
			throw std::runtime_error("small extracted file received non-sequential chunks: " + display_path);
		}

		const auto now = std::chrono::steady_clock::now();
		if (chunk->chunk.length > kBufferedExtractWriteBatchSize) {
			flush_buffer();
			if (chunk->chunk.offset != committed_offset) {
				throw std::runtime_error("small extracted file oversized chunk became non-sequential: " + display_path);
			}
			DWORD bytes_written = 0;
			if (!::WriteFile(
					handle.get(),
					chunk->chunk.data.get(),
					static_cast<DWORD>(chunk->chunk.length),
					&bytes_written,
					nullptr)) {
				throw make_win32_error("failed to write extracted output file: " + display_path);
			}
			if (bytes_written != chunk->chunk.length) {
				throw std::runtime_error("unexpected short write to extracted output file: " + display_path);
			}
			committed_offset += chunk->chunk.length;
			chunk->write_budget_lease.reset();
			expected_offset += chunk->chunk.length;
			continue;
		}

		if (buffered_size != 0
			&& (buffered_size + chunk->chunk.length > kBufferedExtractWriteBatchSize
				|| (fill_started_at.has_value() && now - *fill_started_at >= kBufferedExtractWriteMaxDelay))) {
			flush_buffer();
		}
		if (buffered_size == 0) {
			buffered_offset = chunk->chunk.offset;
			fill_started_at = now;
		}
		std::memcpy(buffered_output.get() + buffered_size, chunk->chunk.data.get(), chunk->chunk.length);
		buffered_size += chunk->chunk.length;
		buffered_leases.push_back(std::move(chunk->write_budget_lease));
		expected_offset += chunk->chunk.length;
		if (buffered_size == kBufferedExtractWriteBatchSize) {
			flush_buffer();
		}
	}
	flush_buffer();

	apply_timestamps_to_handle_if_present(handle.get(), metadata, display_path);
	if (!::CloseHandle(handle.release())) {
		throw make_win32_error("failed to close extracted output file: " + display_path);
	}
	apply_permissions_if_possible(metadata);
	if (file_counter != nullptr) {
		file_counter->fetch_add(1, std::memory_order_relaxed);
	}
}

class ExtractFileWriter {
public:
	ExtractFileWriter(const std::filesystem::path& output_path, std::size_t max_in_flight_write_ops)
		: state_(std::make_unique<State>()) {
		state_->display_path = path_to_utf8_string(output_path);
		state_->max_in_flight_write_ops = std::max<std::size_t>(1, max_in_flight_write_ops);
		state_->handle = ::CreateFileW(
			output_path.c_str(),
			GENERIC_WRITE,
			0,
			nullptr,
			CREATE_ALWAYS,
			FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
			nullptr);
		if (state_->handle == INVALID_HANDLE_VALUE) {
			throw make_win32_error("failed to open extracted output file: " + state_->display_path);
		}

		state_->write_buffer_capacity = kBufferedExtractWriteBatchSize;
		state_->write_slots.reserve(state_->max_in_flight_write_ops + 1);
		for (std::size_t index = 0; index < state_->max_in_flight_write_ops + 1; ++index) {
			state_->write_slots.emplace_back();
			state_->write_slots.back().buffer = make_heap_buffer(state_->write_buffer_capacity);
		}
		state_->active_slot_index = 0;
		for (std::size_t index = 1; index < state_->write_slots.size(); ++index) {
			state_->available_slots.push_back(index);
		}
	}

	~ExtractFileWriter() {
		stop();
	}

	ExtractFileWriter(const ExtractFileWriter&) = delete;
	ExtractFileWriter& operator=(const ExtractFileWriter&) = delete;

	void listenCancelSignal(CancelEvent& event) {
		if (!state_) {
			return;
		}
		state_->cancel_connection = event.connect([this] {
			cancel_pending_work();
		});
	}

	void write(ExtractWriteChunk chunk) {
		if (!state_ || state_->closed || state_->handle == INVALID_HANDLE_VALUE) {
			throw std::runtime_error("extracted output file is closed");
		}
		if (state_->cancel_requested.load(std::memory_order_acquire)) {
			throw CancelledError("extracted output file write cancelled");
		}
		if (chunk.chunk.length == 0) {
			return;
		}

		if (chunk.chunk.length > state_->write_buffer_capacity) {
			submit_active_write();
			write_large_chunk_directly(std::move(chunk));
			return;
		}

		const auto now = std::chrono::steady_clock::now();
		auto& active_slot = state_->write_slots[state_->active_slot_index];
		if (active_slot.size != 0) {
			const bool non_contiguous = chunk.chunk.offset != active_slot.offset + active_slot.size;
			const bool capacity_reached = active_slot.size + chunk.chunk.length > state_->write_buffer_capacity;
			const bool delay_reached = state_->fill_started_at.has_value()
				&& now - *state_->fill_started_at >= kBufferedExtractWriteMaxDelay;
			if (non_contiguous || capacity_reached || delay_reached) {
				submit_active_write();
			}
		}

		auto& current_slot = state_->write_slots[state_->active_slot_index];
		if (current_slot.size == 0) {
			current_slot.offset = chunk.chunk.offset;
			state_->fill_started_at = now;
		}
		std::memcpy(current_slot.buffer.get() + current_slot.size, chunk.chunk.data.get(), chunk.chunk.length);
		current_slot.size += chunk.chunk.length;
		current_slot.write_budget_leases.push_back(std::move(chunk.write_budget_lease));
		if (current_slot.size == state_->write_buffer_capacity) {
			submit_active_write();
		}
	}

	void close(const RestorableMetadata& metadata) {
		if (!state_ || state_->closed) {
			return;
		}
		submit_active_write();
		wait_for_all_writes();
		state_->closed = true;
		apply_timestamps_to_handle_if_present(state_->handle, metadata, state_->display_path);
		if (!::CloseHandle(state_->handle)) {
			state_->handle = INVALID_HANDLE_VALUE;
			throw make_win32_error("failed to close extracted output file: " + state_->display_path);
		}
		state_->handle = INVALID_HANDLE_VALUE;
	}

	void cancel_pending_work() {
		if (!state_) {
			return;
		}
		state_->cancel_requested.store(true, std::memory_order_release);
		if (state_->handle != INVALID_HANDLE_VALUE) {
			::CancelIoEx(state_->handle, nullptr);
		}
	}

private:
	struct State {
		struct WriteSlot : OverlappedSlotBase {
			std::size_t size = 0;
			std::uint64_t offset = 0;
			std::vector<WriteBudgetLease> write_budget_leases;
		};

		HANDLE handle = INVALID_HANDLE_VALUE;
		std::string display_path;
		bool closed = false;
		std::size_t write_buffer_capacity = 0;
		std::size_t max_in_flight_write_ops = 1;
		std::vector<WriteSlot> write_slots;
		std::deque<std::size_t> available_slots;
		std::deque<std::size_t> in_flight_slots;
		std::size_t active_slot_index = 0;
		std::optional<std::chrono::steady_clock::time_point> fill_started_at;
		std::atomic<bool> cancel_requested{false};
		boost::signals2::scoped_connection cancel_connection;
	};

	void stop() noexcept {
		if (!state_) {
			return;
		}
		cancel_pending_work();
		try {
			wait_for_all_writes();
		} catch (...) {
		}
		if (state_->handle != INVALID_HANDLE_VALUE) {
			::CloseHandle(state_->handle);
			state_->handle = INVALID_HANDLE_VALUE;
		}
	}

	void submit_active_write() {
		if (!state_) {
			return;
		}
		if (state_->cancel_requested.load(std::memory_order_acquire)) {
			throw CancelledError("extracted output file write cancelled");
		}

		auto& slot = state_->write_slots[state_->active_slot_index];
		if (slot.size == 0) {
			return;
		}
		if (slot.in_flight) {
			throw std::runtime_error("extracted file write slot is unexpectedly still in flight");
		}

		if (state_->in_flight_slots.size() >= state_->max_in_flight_write_ops) {
			wait_for_one_write();
		}

		slot.overlapped = {};
		slot.overlapped.Offset = static_cast<DWORD>(slot.offset & 0xffffffffull);
		slot.overlapped.OffsetHigh = static_cast<DWORD>((slot.offset >> 32) & 0xffffffffull);
		slot.overlapped.hEvent = slot.event_handle;
		::ResetEvent(slot.event_handle);

		const auto ok = ::WriteFile(
			state_->handle,
			slot.buffer.get(),
			static_cast<DWORD>(slot.size),
			nullptr,
			&slot.overlapped);
		if (!ok && ::GetLastError() != ERROR_IO_PENDING) {
			const auto error = ::GetLastError();
			slot.size = 0;
			slot.offset = 0;
			slot.write_budget_leases.clear();
			if (state_->cancel_requested.load(std::memory_order_acquire)
				&& (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED)) {
				throw CancelledError("extracted output file write cancelled");
			}
			throw make_win32_error("failed to write extracted output file: " + state_->display_path, error);
		}

		slot.in_flight = true;
		state_->in_flight_slots.push_back(state_->active_slot_index);

		if (state_->available_slots.empty()) {
			wait_for_one_write();
		}
		if (state_->available_slots.empty()) {
			throw std::runtime_error("no extracted file write slot available after waiting for completion");
		}

		state_->active_slot_index = state_->available_slots.front();
		state_->available_slots.pop_front();
		state_->write_slots[state_->active_slot_index].size = 0;
		state_->write_slots[state_->active_slot_index].offset = 0;
		state_->write_slots[state_->active_slot_index].write_budget_leases.clear();
		state_->fill_started_at.reset();
	}

	void write_large_chunk_directly(ExtractWriteChunk chunk) {
		wait_for_all_writes();

		OVERLAPPED overlapped{};
		UniqueWin32Handle event(::CreateEventW(nullptr, TRUE, FALSE, nullptr));
		if (!event.valid()) {
			throw make_win32_error("failed to create extracted output write event");
		}
		overlapped.Offset = static_cast<DWORD>(chunk.chunk.offset & 0xffffffffull);
		overlapped.OffsetHigh = static_cast<DWORD>((chunk.chunk.offset >> 32) & 0xffffffffull);
		overlapped.hEvent = event.get();

		const auto ok = ::WriteFile(
			state_->handle,
			chunk.chunk.data.get(),
			static_cast<DWORD>(chunk.chunk.length),
			nullptr,
			&overlapped);
		if (!ok && ::GetLastError() != ERROR_IO_PENDING) {
			const auto error = ::GetLastError();
			if (state_->cancel_requested.load(std::memory_order_acquire)
				&& (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED)) {
				throw CancelledError("extracted output file write cancelled");
			}
			throw make_win32_error("failed to write extracted output file: " + state_->display_path, error);
		}

		DWORD bytes_written = 0;
		if (!::GetOverlappedResult(state_->handle, &overlapped, &bytes_written, TRUE)) {
			const auto error = ::GetLastError();
			if (state_->cancel_requested.load(std::memory_order_acquire)
				&& (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED)) {
				throw CancelledError("extracted output file write cancelled");
			}
			throw make_win32_error("failed to complete extracted output write: " + state_->display_path, error);
		}
		if (bytes_written != chunk.chunk.length) {
			throw std::runtime_error("unexpected short write to extracted output file: " + state_->display_path);
		}
	}

	void wait_for_one_write() {
		if (!state_ || state_->in_flight_slots.empty()) {
			return;
		}

		const auto slot_index = state_->in_flight_slots.front();
		state_->in_flight_slots.pop_front();
		auto& slot = state_->write_slots[slot_index];
		DWORD bytes_written = 0;
		if (!::GetOverlappedResult(state_->handle, &slot.overlapped, &bytes_written, TRUE)) {
			const auto error = ::GetLastError();
			slot.in_flight = false;
			slot.size = 0;
			slot.offset = 0;
			slot.write_budget_leases.clear();
			state_->available_slots.push_back(slot_index);
			if (state_->cancel_requested.load(std::memory_order_acquire)
				&& (error == ERROR_OPERATION_ABORTED || error == ERROR_REQUEST_ABORTED)) {
				throw CancelledError("extracted output file write cancelled");
			}
			throw make_win32_error("failed to complete extracted output write: " + state_->display_path, error);
		}
		if (bytes_written != slot.size) {
			slot.in_flight = false;
			slot.size = 0;
			slot.offset = 0;
			slot.write_budget_leases.clear();
			state_->available_slots.push_back(slot_index);
			throw std::runtime_error("unexpected short write to extracted output file: " + state_->display_path);
		}

		slot.in_flight = false;
		slot.size = 0;
		slot.offset = 0;
		slot.write_budget_leases.clear();
		state_->available_slots.push_back(slot_index);
	}

	void wait_for_all_writes() {
		while (state_ && !state_->in_flight_slots.empty()) {
			wait_for_one_write();
		}
	}

	std::unique_ptr<State> state_;
};

class ExtractWriteScheduler {
public:
	ExtractWriteScheduler(
		const std::filesystem::path& destination_root,
		BufferPool& pool,
		RuntimeExecutors& executors,
		std::shared_ptr<InFlightWriteBudget> write_budget,
		std::size_t max_in_flight_write_ops,
		std::size_t max_parallel_extract_files,
		std::atomic<std::uint64_t>* file_counter,
		const CancelEvent* cancel_event)
		: destination_root_(destination_root),
		  pool_(pool),
		  executors_(executors),
		  write_budget_(std::move(write_budget)),
		  max_in_flight_write_ops_(std::max<std::size_t>(1, max_in_flight_write_ops)),
		  max_parallel_extract_files_(std::max<std::size_t>(1, max_parallel_extract_files)),
		  file_counter_(file_counter),
		  cancel_event_(cancel_event),
		  directory_index_(cancel_event) {
		directory_index_.mark_created(destination_root_);
	}

	void handle_directory_entry(RestorableMetadata metadata) {
		throw_if_cancelled(cancel_event_);
		directory_index_.ensure_directory_exists(metadata.output_path);
		directory_finalizer_.record(std::move(metadata));
	}

	void begin_regular_file(RestorableMetadata metadata) {
		throw_if_cancelled(cancel_event_);
		finish_current_file();
		drain_completed_tasks();

		const auto path_key = metadata.output_path.wstring();
		wait_for_matching_path(path_key);
		while (active_tasks_.size() >= max_parallel_extract_files_) {
			wait_for_any_task();
		}

		directory_index_.ensure_directory_exists(metadata.output_path.parent_path());

		auto queue = std::make_shared<BoundedQueue<ExtractWriteChunk>>(
			std::max<std::size_t>(2, max_in_flight_write_ops_ + 1),
			executors_.executor());
		if (cancel_event_ != nullptr) {
			queue->listenCancelSignal(*const_cast<CancelEvent*>(cancel_event_));
		}
		current_file_path_key_ = path_key;
		current_file_queue_ = queue;

		const auto task_id = next_task_id_++;
		auto future = executors_.post([this, task_id, queue, metadata = std::move(metadata), max_in_flight_write_ops = max_in_flight_write_ops_, file_counter = file_counter_, cancel_event = cancel_event_] {
			try {
				ExtractFileWriter writer(metadata.output_path, max_in_flight_write_ops);
				if (cancel_event != nullptr) {
					writer.listenCancelSignal(*const_cast<CancelEvent*>(cancel_event));
				}
				while (auto chunk = queue->pop()) {
					throw_if_cancelled(cancel_event);
					writer.write(std::move(*chunk));
				}
				writer.close(metadata);
				apply_permissions_if_possible(metadata);
				if (file_counter != nullptr) {
					file_counter->fetch_add(1, std::memory_order_relaxed);
				}
			} catch (...) {
				queue->abandon();
				notify_task_completed(task_id);
				throw;
			}
			notify_task_completed(task_id);
		});

		auto it = active_tasks_.emplace(active_tasks_.end(), ActiveFileTask{
			task_id,
			path_key,
			std::move(queue),
			std::move(future),
		});
		active_tasks_by_path_[path_key] = it;
		active_tasks_by_id_[task_id] = it;
	}

	void push_current_block(const void* buffer, std::size_t length, std::uint64_t offset) {
		if (!current_file_queue_) {
			throw std::runtime_error("no active extracted file queue");
		}
		if (length == 0) {
			return;
		}

		write_budget_->acquire(length);
		try {
			auto owned_buffer = pool_.acquire(length);
			std::memcpy(owned_buffer.get(), buffer, length);
			ExtractWriteChunk chunk;
			chunk.chunk = DataChunk{std::move(owned_buffer), length, offset, false};
			chunk.write_budget_lease = WriteBudgetLease(write_budget_, length);
			current_file_queue_->push(std::move(chunk));
		} catch (...) {
			write_budget_->release(length);
			throw;
		}
	}

	void finish_current_file() {
		if (!current_file_queue_) {
			return;
		}
		current_file_queue_->close();
		current_file_queue_.reset();
		current_file_path_key_.clear();
	}

	void finalize() {
		finish_current_file();
		while (!active_tasks_.empty()) {
			wait_for_any_task();
		}
		directory_finalizer_.apply_all(cancel_event_);
	}

	void abort() {
		if (current_file_queue_) {
			current_file_queue_->abandon();
			current_file_queue_.reset();
			current_file_path_key_.clear();
		}
		for (auto& task : active_tasks_) {
			task.queue->abandon();
		}
	}

	void wait_for_all_noexcept() noexcept {
		while (!active_tasks_.empty()) {
			try {
				wait_for_any_task();
			} catch (...) {
			}
		}
	}

private:
	struct ActiveFileTask {
		std::uint64_t task_id = 0;
		std::wstring path_key;
		std::shared_ptr<BoundedQueue<ExtractWriteChunk>> queue;
		std::future<void> future;
	};

	void notify_task_completed(std::uint64_t task_id) {
		{
			std::lock_guard lock(completed_mutex_);
			completed_task_ids_.push_back(task_id);
		}
		completed_cv_.notify_one();
	}

	void drain_completed_tasks() {
		std::deque<std::uint64_t> completed_ids;
		{
			std::lock_guard lock(completed_mutex_);
			completed_ids.swap(completed_task_ids_);
		}
		for (const auto task_id : completed_ids) {
			auto found = active_tasks_by_id_.find(task_id);
			if (found != active_tasks_by_id_.end()) {
				wait_for_task(found->second);
			}
		}
	}

	void wait_for_matching_path(const std::wstring& path_key) {
		auto found = active_tasks_by_path_.find(path_key);
		if (found == active_tasks_by_path_.end()) {
			return;
		}
		wait_for_task(found->second);
	}

	void wait_for_any_task() {
		for (;;) {
			std::uint64_t completed_task_id = 0;
			{
				std::unique_lock lock(completed_mutex_);
				completed_cv_.wait_for(lock, std::chrono::milliseconds(50), [&] {
					return !completed_task_ids_.empty() || (cancel_event_ != nullptr && cancel_event_->is_cancelled());
				});
				throw_if_cancelled(cancel_event_);
				if (completed_task_ids_.empty()) {
					continue;
				}
				completed_task_id = completed_task_ids_.front();
				completed_task_ids_.pop_front();
			}

			auto found = active_tasks_by_id_.find(completed_task_id);
			if (found != active_tasks_by_id_.end()) {
				wait_for_task(found->second);
				return;
			}
		}
	}

	void wait_for_task(std::list<ActiveFileTask>::iterator task_it) {
		task_it->future.get();
		active_tasks_by_id_.erase(task_it->task_id);
		active_tasks_by_path_.erase(task_it->path_key);
		active_tasks_.erase(task_it);
	}

	std::filesystem::path destination_root_;
	BufferPool& pool_;
	RuntimeExecutors& executors_;
	std::shared_ptr<InFlightWriteBudget> write_budget_;
	std::size_t max_in_flight_write_ops_ = 1;
	std::size_t max_parallel_extract_files_ = 1;
	std::atomic<std::uint64_t>* file_counter_ = nullptr;
	const CancelEvent* cancel_event_ = nullptr;
	CreatedDirectoryIndex directory_index_;
	DirectoryMetadataFinalizer directory_finalizer_;
	std::shared_ptr<BoundedQueue<ExtractWriteChunk>> current_file_queue_;
	std::wstring current_file_path_key_;
	std::list<ActiveFileTask> active_tasks_;
	std::unordered_map<std::uint64_t, std::list<ActiveFileTask>::iterator> active_tasks_by_id_;
	std::unordered_map<std::wstring, std::list<ActiveFileTask>::iterator> active_tasks_by_path_;
	std::uint64_t next_task_id_ = 1;
	std::mutex completed_mutex_;
	std::condition_variable completed_cv_;
	std::deque<std::uint64_t> completed_task_ids_;
};

} // namespace

TarPacker::TarPacker(BufferPool& pool, std::size_t chunk_size)
	: pool_(pool), chunk_size_(chunk_size) {}

void TarPacker::pack(
	BoundedQueue<OpenedFileReader>& in_meta,
	BoundedQueue<DataChunk>& out_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	TarWriteContext context{&out_tar, &pool_, 0, uncompressed_bytes_counter, cancel_event};

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
			throw_if_cancelled(cancel_event);
			meta->read_budget_lease.reset();
			add_entry(writer, *meta, file_counter, cancel_event);
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


void TarPacker::add_entry(struct archive* writer, OpenedFileReader& opened_file, std::atomic<std::uint64_t>* file_counter, const CancelEvent* cancel_event) const {
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
#if 0
	} else if (status_type == std::filesystem::file_type::symlink) {
		if (!meta.symlink_target.has_value()) {
			archive_entry_free(entry);
			throw std::runtime_error("failed to resolve symlink target for tar entry: " + meta.relative_path_in_tar);
		}
		archive_entry_set_filetype(entry, AE_IFLNK);
		archive_entry_set_size(entry, 0);
		archive_entry_set_symlink_utf8(entry, meta.symlink_target->c_str());
	} else {
#endif
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

TarUnpacker::TarUnpacker(
	const std::filesystem::path& destination_root,
	BufferPool& pool,
	RuntimeExecutors& executors,
	std::shared_ptr<InFlightWriteBudget> write_budget,
	std::size_t max_in_flight_write_ops,
	std::size_t max_parallel_extract_files)
	: destination_root_(destination_root),
	  pool_(pool),
	  executors_(executors),
	  write_budget_(std::move(write_budget)),
	  max_in_flight_write_ops_(std::max<std::size_t>(1, max_in_flight_write_ops)),
	  max_parallel_extract_files_(std::max<std::size_t>(1, max_parallel_extract_files)) {
	std::filesystem::create_directories(destination_root_);
}

void TarUnpacker::unpack(
	BoundedQueue<DataChunk>& in_tar,
	std::atomic<std::uint64_t>* uncompressed_bytes_counter,
	std::atomic<std::uint64_t>* file_counter,
	const CancelEvent* cancel_event) {
	TarReadContext context{&in_tar, std::nullopt, 0, uncompressed_bytes_counter, cancel_event};
	auto* reader = archive_read_new();
	if (reader == nullptr) {
		throw std::runtime_error("failed to allocate libarchive reader");
	}

	ExtractWriteScheduler scheduler(
		destination_root_,
		pool_,
		executors_,
		write_budget_,
		max_in_flight_write_ops_,
		max_parallel_extract_files_,
		file_counter,
		cancel_event);

	try {
		if (archive_read_support_format_tar(reader) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to enable tar format support");
		}
		if (archive_read_open(reader, &context, nullptr, &TarUnpacker::archive_read_callback, &TarUnpacker::archive_close_callback) != ARCHIVE_OK) {
			throw_archive_error(reader, "failed to open tar input stream");
		}

		archive_entry* entry = nullptr;
		while (true) {
			throw_if_cancelled(cancel_event);
			const auto status = archive_read_next_header(reader, &entry);
			if (status == ARCHIVE_EOF) {
				break;
			}
			if (status != ARCHIVE_OK) {
				throw_archive_error(reader, "failed to read tar header");
			}
			if (archive_entry_filetype(entry) == AE_IFLNK) {
				archive_read_data_skip(reader);
				continue;
			}

			const auto* utf8_name = archive_entry_pathname_utf8(entry);
			if (utf8_name == nullptr) {
				throw std::runtime_error("archive entry has non-UTF-8 or undecodable pathname");
			}
			const auto output_path = resolve_output_path(utf8_name);
			auto metadata = capture_restorable_metadata(entry, output_path);
			const auto file_type = archive_entry_filetype(entry);
			if (file_type == AE_IFDIR) {
				scheduler.handle_directory_entry(std::move(metadata));
				continue;
			}
			if (file_type != AE_IFREG) {
				archive_read_data_skip(reader);
				continue;
			}

			scheduler.begin_regular_file(std::move(metadata));
			const void* buffer = nullptr;
			size_t length = 0;
			la_int64_t offset = 0;
			while (true) {
				throw_if_cancelled(cancel_event);
				const auto block_status = archive_read_data_block(reader, &buffer, &length, &offset);
				if (block_status == ARCHIVE_EOF) {
					break;
				}
				if (block_status != ARCHIVE_OK) {
					throw_archive_error(reader, "failed to stream tar payload");
				}
				scheduler.push_current_block(buffer, length, static_cast<std::uint64_t>(offset));
			}
			scheduler.finish_current_file();
		}

		scheduler.finalize();
		archive_read_close(reader);
		archive_read_free(reader);
	} catch (...) {
		scheduler.abort();
		scheduler.wait_for_all_noexcept();
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
	auto relative_path = normalize_relative_path(std::filesystem::path(reinterpret_cast<const char8_t*>(utf8_path)));
	if (relative_path == ".") {
		return destination_root_;
	}
	return destination_root_ / relative_path;
}

} // namespace soratransport
