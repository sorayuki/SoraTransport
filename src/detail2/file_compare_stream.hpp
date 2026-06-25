#pragma once

#include "filesystem.hpp"
#include "protocol.hpp"

#include "../detail/io.hpp"

#include <filesystem>
#include <vector>

namespace soratransport::detail2 {

void stream_file_comparison_to_opener(
	BoundedQueue<TraversalEntry>& traversal_queue,
	BoundedQueue<TraversalEntry>& filtered_traversal_queue,
	SocketByteSink& sink,
	CancelEvent& cancel_event);

void handle_file_comparison_control_message(
	std::string_view control_json,
	const std::filesystem::path& destination_dir,
	SocketByteSource& source,
	TaskExecutor& compare_executor);

} // namespace soratransport::detail2
