#include "file_compare_stream.hpp"

#include "../detail/internal.hpp"

#include <boost/asio/system_executor.hpp>

#include <algorithm>
#include <exception>
#include <future>
#include <thread>
#include <unordered_set>

namespace soratransport::detail2 {

namespace {

constexpr std::size_t kFileInfoBatchSize = 256;
constexpr std::size_t kTraversalBatchSize = 512;

struct PendingTraversalBatch : IQueueDisposable {
	void Dispose() noexcept override {
		file_infos.clear();
		entries.clear();
	}

	std::vector<control_msg::FileInfoEntry> file_infos;
	std::vector<TraversalEntry> entries;
};

control_msg::FileInfoEntry file_info_for_entry(TraversalEntry& entry) {
	if (!entry.meta.last_write_time.has_value()) {
		populate_file_metadata(entry.meta);
	}

	control_msg::FileInfoEntry info;
	info.relative_path = entry.meta.relative_path_in_tar;
	info.size = entry.meta.size;
	if (entry.meta.last_write_time) {
		info.mtime_sec = entry.meta.last_write_time->seconds;
		info.mtime_nsec = entry.meta.last_write_time->nanoseconds;
	}
	return info;
}

void produce_comparison_batches(
	BoundedQueue<TraversalEntry>& traversal_queue,
	BoundedQueue<PendingTraversalBatch>& pending_batches,
	CancelEvent& cancel_event) {
	try {
		PendingTraversalBatch batch;
		while (auto entry_opt = traversal_queue.pop()) {
			if (cancel_event.is_cancelled()) {
				throw CancelledError();
			}

			auto entry = std::move(*entry_opt);
			if (entry.regular_file) {
				batch.file_infos.push_back(file_info_for_entry(entry));
			}
			batch.entries.push_back(std::move(entry));
			if (batch.file_infos.size() >= kFileInfoBatchSize
				|| batch.entries.size() >= kTraversalBatchSize) {
				pending_batches.push(std::move(batch));
				batch = {};
			}
		}

		if (!batch.entries.empty()) {
			pending_batches.push(std::move(batch));
		}
		pending_batches.close();
	} catch (...) {
		pending_batches.close();
		throw;
	}
}

void forward_entry(
	TraversalEntry entry,
	BoundedQueue<TraversalEntry>& filtered_traversal_queue,
	std::size_t& next_filtered_sequence) {
	entry.sequence = next_filtered_sequence++;
	filtered_traversal_queue.push(std::move(entry));
}

void send_batches_and_forward_diffs(
	BoundedQueue<PendingTraversalBatch>& pending_batches,
	BoundedQueue<TraversalEntry>& filtered_traversal_queue,
	SocketByteSink& sink,
	CancelEvent& cancel_event,
	std::size_t& next_filtered_sequence) {
	try {
		while (auto batch_opt = pending_batches.pop()) {
			if (cancel_event.is_cancelled()) {
				throw CancelledError();
			}

			auto batch = std::move(*batch_opt);
			std::unordered_set<std::string> diff_paths;
			if (!batch.file_infos.empty()) {
				auto batch_json = control_msg::serialize_file_info_batch(
					control_msg::kTypeFileInfoBatch,
					batch.file_infos);
				sink.send_control_message(batch_json);
				sink.flush_control_buffer();

				const auto response_json = sink.await_control_response();
				const auto response_type = control_msg::control_message_type(response_json);
				if (response_type != control_msg::kTypeFileInfoDiff) {
					throw std::runtime_error("expected file_info_diff response but received: " + response_type);
				}

				auto diff_entries = control_msg::deserialize_file_info_batch(response_json);
				diff_paths.reserve(diff_entries.size());
				for (auto& diff : diff_entries) {
					diff_paths.insert(std::move(diff.relative_path));
				}
			}

			for (auto& entry : batch.entries) {
				if (!entry.regular_file || diff_paths.contains(entry.meta.relative_path_in_tar)) {
					forward_entry(std::move(entry), filtered_traversal_queue, next_filtered_sequence);
				}
			}
		}

		sink.send_control_message(control_msg::serialize_file_info_end());
		sink.flush_control_buffer();
		filtered_traversal_queue.close();
	} catch (...) {
		filtered_traversal_queue.close();
		throw;
	}
}

std::vector<control_msg::FileInfoEntry> compare_file_info_batch(
	const std::vector<control_msg::FileInfoEntry>& file_infos,
	const std::filesystem::path& destination_dir,
	TaskExecutor& compare_executor) {
	constexpr std::size_t kCompareBatchSize = 64;
	std::vector<std::future<bool>> compare_futures;
	compare_futures.reserve(file_infos.size());

	for (std::size_t batch_start = 0; batch_start < file_infos.size(); batch_start += kCompareBatchSize) {
		const auto batch_end = std::min(batch_start + kCompareBatchSize, file_infos.size());

		for (std::size_t i = batch_start; i < batch_end; ++i) {
			const auto info = file_infos[i];
			compare_futures.push_back(compare_executor.submit(
				[&dest = destination_dir, info]() -> bool {
					return file_differs_from_local(
						dest,
						info.relative_path,
						info.size,
						info.mtime_sec,
						info.mtime_nsec);
				}));
		}

		for (std::size_t i = batch_start; i < batch_end; ++i) {
			compare_futures[i].wait();
		}
	}

	std::vector<control_msg::FileInfoEntry> diff_files;
	for (std::size_t i = 0; i < file_infos.size(); ++i) {
		if (compare_futures[i].get()) {
			diff_files.push_back(file_infos[i]);
		}
	}
	return diff_files;
}

} // namespace

void stream_file_comparison_to_opener(
	BoundedQueue<TraversalEntry>& traversal_queue,
	BoundedQueue<TraversalEntry>& filtered_traversal_queue,
	SocketByteSink& sink,
	CancelEvent& cancel_event) {
	BoundedQueue<PendingTraversalBatch> pending_batches(16, boost::asio::system_executor());
	pending_batches.listenCancelSignal(cancel_event);

	std::size_t next_filtered_sequence = 0;
	std::exception_ptr producer_error;
	std::jthread producer([&] {
		try {
			produce_comparison_batches(
				traversal_queue,
				pending_batches,
				cancel_event);
		} catch (...) {
			producer_error = std::current_exception();
		}
	});

	std::exception_ptr consumer_error;
	try {
		send_batches_and_forward_diffs(
			pending_batches,
			filtered_traversal_queue,
			sink,
			cancel_event,
			next_filtered_sequence);
	} catch (...) {
		consumer_error = std::current_exception();
		traversal_queue.abandon();
		pending_batches.abandon();
		filtered_traversal_queue.close();
	}

	if (producer.joinable()) {
		producer.join();
	}
	if (consumer_error) {
		std::rethrow_exception(consumer_error);
	}
	if (producer_error) {
		std::rethrow_exception(producer_error);
	}
}

void handle_file_comparison_control_message(
	std::string_view control_json,
	const std::filesystem::path& destination_dir,
	SocketByteSource& source,
	TaskExecutor& compare_executor) {
	const auto message_type = control_msg::control_message_type(control_json);
	if (message_type == control_msg::kTypeFileInfoEnd) {
		return;
	}
	if (message_type != control_msg::kTypeFileInfoBatch) {
		throw std::runtime_error("unexpected file comparison control message: " + message_type);
	}

	auto file_infos = control_msg::deserialize_file_info_batch(control_json);
	auto diff_files = compare_file_info_batch(file_infos, destination_dir, compare_executor);
	auto diff_json = control_msg::serialize_file_info_batch(
		control_msg::kTypeFileInfoDiff,
		diff_files);
	source.send_control_message(diff_json);
}

} // namespace soratransport::detail2
