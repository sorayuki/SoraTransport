#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <stdexcept>
#include <vector>

namespace soratransport::detail2 {

// ============================================================================
// TransportFrameHeader — 每条 WebSocket 消息载荷前 8 字节
// ============================================================================
struct TransportFrameHeader {
	static constexpr uint8_t kMagic0 = 'S';
	static constexpr uint8_t kMagic1 = 'T';
	static constexpr uint8_t kVersion = 1;

	enum Channel : uint8_t {
		kData    = 0,  // 数据通道：zstd 压缩 tar 字节流
		kControl = 1,  // 控制通道：JSON 控制消息
	};

	static constexpr std::size_t kSerializedSize = 8;

	uint8_t  magic[2]   = {kMagic0, kMagic1};
	uint8_t  version     = kVersion;
	uint8_t  channel     = kData;
	uint32_t payload_size = 0;  // big-endian

	static TransportFrameHeader make(Channel ch, std::size_t payload_bytes);

	// 序列化为 8 字节网络序
	void serialize_to(std::span<uint8_t, kSerializedSize> out) const;

	// 从 8 字节反序列化，验证 magic / version
	static TransportFrameHeader deserialize_from(std::span<const uint8_t, kSerializedSize> in);

	// 便捷：同时构造 header + 写入 buffer
	static void write_header(std::vector<uint8_t>& buffer, Channel ch, std::size_t payload_bytes);
};

// ============================================================================
// ControlChannel — 控制通道 JSON 消息类型定义
// ============================================================================
namespace control_msg {

// 已定义：transport_begin / transport_end 复用现有 JSON 约定
constexpr std::string_view kTypeTransportBegin = "transport_begin";
constexpr std::string_view kTypeTransportEnd   = "transport_end";

// 新定义：文件信息批量交换
constexpr std::string_view kTypeFileInfoBatch = "file_info_batch";  // 发送端 → 接收端
constexpr std::string_view kTypeFileInfoDiff  = "file_info_diff";   // 接收端 → 发送端

// 文件信息条目
struct FileInfoEntry {
	std::string relative_path;       // UTF-8 tar 相对路径
	std::uint64_t size = 0;         // 文件大小
	std::int64_t mtime_sec = 0;     // 最后修改时间 (秒)
	long mtime_nsec = 0;           // 最后修改时间 (纳秒)
};

// 将 FileInfoEntry 列表序列化为 JSON
std::string serialize_file_info_batch(
	std::string_view msg_type,
	const std::vector<FileInfoEntry>& files);

// 反序列化 JSON 为 FileInfoEntry 列表
std::vector<FileInfoEntry> deserialize_file_info_batch(std::string_view json_text);

} // namespace control_msg

} // namespace soratransport::detail2
