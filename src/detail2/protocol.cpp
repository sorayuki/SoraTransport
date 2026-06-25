#include "protocol.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstring>

namespace soratransport::detail2 {

// ============================================================================
// TransportFrameHeader
// ============================================================================

TransportFrameHeader TransportFrameHeader::make(Channel ch, std::size_t payload_bytes) {
	TransportFrameHeader hdr;
	hdr.channel = ch;
	hdr.payload_size = static_cast<uint32_t>(payload_bytes);
	return hdr;
}

void TransportFrameHeader::serialize_to(std::span<uint8_t, kSerializedSize> out) const {
	out[0] = magic[0];
	out[1] = magic[1];
	out[2] = version;
	out[3] = channel;
	// payload_size big-endian
	out[4] = static_cast<uint8_t>((payload_size >> 24) & 0xff);
	out[5] = static_cast<uint8_t>((payload_size >> 16) & 0xff);
	out[6] = static_cast<uint8_t>((payload_size >> 8) & 0xff);
	out[7] = static_cast<uint8_t>(payload_size & 0xff);
}

TransportFrameHeader TransportFrameHeader::deserialize_from(std::span<const uint8_t, kSerializedSize> in) {
	if (in[0] != kMagic0 || in[1] != kMagic1) {
		throw std::runtime_error("invalid transport frame magic");
	}
	if (in[2] != kVersion) {
		throw std::runtime_error("unsupported transport frame version");
	}
	TransportFrameHeader hdr;
	hdr.magic[0] = in[0];
	hdr.magic[1] = in[1];
	hdr.version = in[2];
	hdr.channel = in[3];
	hdr.payload_size = (static_cast<uint32_t>(in[4]) << 24)
	                 | (static_cast<uint32_t>(in[5]) << 16)
	                 | (static_cast<uint32_t>(in[6]) << 8)
	                 | static_cast<uint32_t>(in[7]);
	return hdr;
}

void TransportFrameHeader::write_header(std::vector<uint8_t>& buffer, Channel ch, std::size_t payload_bytes) {
	const auto hdr = make(ch, payload_bytes);
	const auto old_size = buffer.size();
	buffer.resize(old_size + kSerializedSize);
	std::span<uint8_t, kSerializedSize> out(buffer.data() + old_size, kSerializedSize);
	hdr.serialize_to(out);
}

// ============================================================================
// ControlChannel
// ============================================================================

namespace control_msg {

std::string serialize_file_info_batch(
	std::string_view msg_type,
	const std::vector<FileInfoEntry>& files) {
	nlohmann::json payload;
	payload["type"] = msg_type;
	auto& json_files = payload["files"] = nlohmann::json::array();
	for (const auto& entry : files) {
		nlohmann::json file_obj;
		file_obj["path"] = entry.relative_path;
		file_obj["size"] = entry.size;
		file_obj["mtime_sec"] = entry.mtime_sec;
		file_obj["mtime_nsec"] = entry.mtime_nsec;
		json_files.push_back(std::move(file_obj));
	}
	return payload.dump();
}

std::string serialize_file_info_end() {
	nlohmann::json payload;
	payload["type"] = kTypeFileInfoEnd;
	return payload.dump();
}

std::string control_message_type(std::string_view json_text) {
	auto payload = nlohmann::json::parse(json_text);
	if (!payload.is_object()) {
		throw std::runtime_error("control message must be a JSON object");
	}
	const auto type_it = payload.find("type");
	if (type_it == payload.end() || !type_it->is_string()) {
		throw std::runtime_error("control message missing 'type' field");
	}
	return type_it->get<std::string>();
}

std::vector<FileInfoEntry> deserialize_file_info_batch(std::string_view json_text) {
	auto payload = nlohmann::json::parse(json_text);
	if (!payload.is_object()) {
		throw std::runtime_error("file info batch must be a JSON object");
	}

	const auto type_it = payload.find("type");
	if (type_it == payload.end() || !type_it->is_string()) {
		throw std::runtime_error("file info batch missing 'type' field");
	}
	const auto msg_type = type_it->get<std::string>();
	if (msg_type != kTypeFileInfoBatch && msg_type != kTypeFileInfoDiff) {
		throw std::runtime_error("unexpected file info list type: " + msg_type);
	}

	const auto files_it = payload.find("files");
	if (files_it == payload.end() || !files_it->is_array()) {
		throw std::runtime_error("file info batch missing 'files' array");
	}

	std::vector<FileInfoEntry> result;
	result.reserve(files_it->size());
	for (const auto& obj : *files_it) {
		FileInfoEntry entry;
		entry.relative_path = obj.value("path", "");
		entry.size = obj.value("size", 0ull);
		entry.mtime_sec = obj.value("mtime_sec", 0ll);
		entry.mtime_nsec = obj.value("mtime_nsec", 0l);
		if (!entry.relative_path.empty()) {
			result.push_back(std::move(entry));
		}
	}
	return result;
}

} // namespace control_msg

} // namespace soratransport::detail2
