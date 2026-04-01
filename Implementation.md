# SoraTransport Implementation Notes

本文档描述当前代码库的实际实现状态，作为 [Design.md](Design.md) 的落地补充。`Design.md` 更偏向目标架构与设计意图，本文档聚焦当前代码如何组织、模块之间如何协作，以及后续维护时需要注意的实现细节。

## 1. 当前实现概览

当前工程提供两个可执行程序：

- `soratransport`
  - `pack <source-dir> <output.tar.zst>`
  - `unpack <input.tar.zst> <destination-dir>`
  - `send <source-dir> <host> <port>`
  - `receive <port> <destination-dir>`
- `fasttar`
  - `pack [--zstd|--no-compress] <source-dir> <output.tar|output.tar.zst>`
  - `unpack [--zstd|--no-compress] <input.tar|input.tar.zst> <destination-dir>`

实现语言为 C++20，核心依赖包括：

- Boost.Asio：网络、线程池、协程控制平面
- libarchive：tar 打包与解包
- zstd：流式压缩与解压
- Windows API：大块文件读取时的内存映射快速路径

## 2. 目录结构

当前核心代码已经从单一的 `core.cpp` / `core.hpp` 拆分为多个模块文件。

### 2.1 公开入口

- [src/core.hpp](src/core.hpp)
  - 对外聚合头文件
  - 暴露顶层 API：`pack_directory_to_file`、`unpack_file_to_directory`、`send_directory`、`receive_directory`、CLI 入口函数
- [src/core.cpp](src/core.cpp)
  - 兼容空单元，保留库目标中的稳定入口文件名

### 2.2 细分模块

- [src/detail/types.hpp](src/detail/types.hpp)
  - 基础数据类型：`DataChunk`、`FileMeta`、`CompressionMode`
  - 通道与队列基础设施：`BoundedQueue<T>`、`BlockingChannel<T>`
- [src/detail/runtime.hpp](src/detail/runtime.hpp)
  - `BufferPool`
  - `RuntimeExecutors`
- [src/detail/runtime.cpp](src/detail/runtime.cpp)
  - 运行时配置计算
  - 线程池实现
  - zstd worker 配置辅助
  - `PipelineState`
- [src/detail/internal.hpp](src/detail/internal.hpp)
  - 仅供内部实现文件共享的辅助声明
- [src/detail/io.hpp](src/detail/io.hpp)
  - 字节源/汇接口：`IByteSource`、`IByteSink`
  - 文件与 socket 适配器声明
- [src/detail/io.cpp](src/detail/io.cpp)
  - `FileByteSink`、`FileByteSource`
  - `SocketByteSink`、`SocketByteSource`
- [src/detail/filesystem.cpp](src/detail/filesystem.cpp)
  - `DirScanner`
  - `FileReader`
  - 目录工作队列与 Windows mmap 读取快路径
- [src/detail/tar.cpp](src/detail/tar.cpp)
  - `TarPacker`
  - `TarUnpacker`
  - tar 读写上下文与归档相关辅助逻辑
- [src/detail/zstd.cpp](src/detail/zstd.cpp)
  - `ZstdCompressor`
  - `ZstdDecompressor`
  - `RawTarWriter`
  - `RawTarReader`
- [src/detail/orchestration.cpp](src/detail/orchestration.cpp)
  - 顶层本地打包/解包编排
  - 顶层网络发送/接收编排
  - 网络协程控制流
- [src/detail/cli.cpp](src/detail/cli.cpp)
  - `soratransport` CLI 解析
  - `fasttar` CLI 解析
  - 使用说明输出

## 3. 实际数据流

### 3.1 本地打包

`pack_directory_to_file()` 的执行流程：

1. 创建运行时配置和线程池。
2. `DirScanner` 扫描目录，将 `FileMeta` 推入 `BoundedQueue<FileMeta>`。
3. `TarPacker` 从元数据队列读取条目，驱动 `FileReader` 并调用 libarchive 输出 tar 数据。
4. tar 数据进入 `ConcurrentDataChunkChannel`。
5. 若模式为 `Zstd`，由 `ZstdCompressor` 读取 tar 数据并写入目标输出；否则由 `RawTarWriter` 直接落盘。

### 3.2 本地解包

`unpack_file_to_directory()` 的执行流程：

1. 创建运行时配置和线程池。
2. 根据输入模式选择：
   - `ZstdDecompressor`：先解压再输出 tar 数据
   - `RawTarReader`：直接读取 tar 数据
3. tar 数据进入 `ConcurrentDataChunkChannel`。
4. `TarUnpacker` 使用 libarchive 读取 tar 数据并写入磁盘。

### 3.3 网络发送

`send_directory()` 使用 Boost.Asio 协程建立连接，之后进入与本地打包相同的数据面：

1. 扫描目录
2. 打 tar
3. zstd 压缩
4. 通过 `SocketByteSink` 发送字节流

### 3.4 网络接收

`receive_directory()` 使用 Boost.Asio 协程监听并接收 socket，之后进入逆向流水线：

1. `SocketByteSource` 从网络读取数据
2. `ZstdDecompressor` 解压
3. `TarUnpacker` 还原文件树

## 4. 并发模型与线程策略

当前实现使用的是“协程控制平面 + 线程化数据面”的组合，而不是完整的端到端 awaitable stage 流转。

### 4.1 当前线程池划分

- scanner 线程：目录遍历
- reader 线程池：文件读取和 mmap 相关阻塞操作
- compression 线程池：zstd 压缩

运行时配置在 [src/detail/runtime.cpp](src/detail/runtime.cpp) 中由 `make_runtime_config()` 根据硬件线程数自动计算，重点参数包括：

- `scanner_threads`
- `reader_threads`
- `compression_threads`
- `read_concurrency`
- `tar_queue_depth`

### 4.2 当前通道实现

当前代码没有继续依赖 Boost 的实验性 `concurrent_channel`，而是使用 [src/detail/types.hpp](src/detail/types.hpp) 中的 `BlockingChannel<T>` 作为线程安全阻塞通道实现。

这么做的原因是：

- 避免依赖实验性接口
- 避免事件循环线程与同步等待混用时的死锁风险
- 保持现有数据面阶段之间的背压语义

注意：虽然别名仍叫 `ConcurrentDataChunkChannel`，但其底层实现已经是项目内自定义的阻塞式通道，而不是 Boost.Experimental API。

## 5. 文件读取与写入细节

### 5.1 FileReader

[src/detail/filesystem.cpp](src/detail/filesystem.cpp) 中的 `FileReader` 有两条路径：

- 常规路径：`std::ifstream` 分块读取
- Windows 快路径：当块大小达到 `kPipelineChunkSize` 时，优先尝试 `CreateFileMappingW + MapViewOfFile`

这条快路径的目标是提升大文件顺序读取吞吐，减少高频小读的系统调用开销。

### 5.2 TarUnpacker

解包阶段使用 libarchive 的 `archive_write_disk` 写入磁盘，而不是手工解析 tar 再写文件。这么做的优势是：

- 目录、权限、时间戳处理更稳定
- 对嵌套目录和连续流式输入更可靠

## 6. 压缩实现细节

### 6.1 ZstdCompressor

[src/detail/zstd.cpp](src/detail/zstd.cpp) 中的 `ZstdCompressor` 使用 `ZSTD_compressStream2()` 进行流式压缩。

实现要点：

- 直接消费 tar 数据块，不按单文件重启压缩上下文
- 根据 `RuntimeExecutors::compression_threads()` 尝试配置 `ZSTD_c_nbWorkers`
- 若 zstd 当前构建不支持该参数，则安全降级为单 worker，不视为错误

### 6.2 ZstdDecompressor

解压使用 `ZSTD_decompressStream()`，持续从输入源读取字节并输出 tar 数据块，直到输入 EOF 且 zstd 状态归零。

## 7. CLI 约定

### 7.1 soratransport

当前 `soratransport` 的 pack/unpack 仍然固定使用 zstd 压缩格式，对应 `.tar.zst`。

### 7.2 fasttar

当前 `fasttar` 支持两种模式：

- 显式指定：
  - `--zstd`
  - `--no-compress`
- 自动推断：若未显式指定，则根据输入或输出扩展名推断

规则为：

- `.tar` => `CompressionMode::None`
- `.tar.zst` / `.tzst` / `.zst` => `CompressionMode::Zstd`

CLI 解析逻辑位于 [src/detail/cli.cpp](src/detail/cli.cpp)。

## 8. 构建方式

构建入口在 [CMakeLists.txt](CMakeLists.txt)。

`soratransport_core` 当前编译单元包括：

- `src/core.cpp`
- `src/detail/runtime.cpp`
- `src/detail/io.cpp`
- `src/detail/filesystem.cpp`
- `src/detail/tar.cpp`
- `src/detail/zstd.cpp`
- `src/detail/orchestration.cpp`
- `src/detail/cli.cpp`

其中 `src/core.cpp` 主要用于维持稳定的库入口文件名和外部引用兼容性。

## 9. 当前实现与设计文档的差异

和 [Design.md](Design.md) 相比，当前实现有几个重要差异：

### 9.1 已落地部分

- 目录扫描、文件读取、tar 打包/解包、zstd、网络传输都已实现
- 本地打包、解包和网络收发都已可用
- 模块已按职责拆分为多个编译单元
- 线程数与压缩 worker 数支持运行时自适应

### 9.2 有意保守的部分

- 阶段之间的通道目前是项目内阻塞式通道，而不是完整的协程消息通道
- 数据面主要依赖线程与阻塞队列/通道，而不是全链路 awaitable stage
- `soratransport` CLI 还未暴露可选压缩开关，只有 `fasttar` 提供此能力

## 10. 后续可继续优化的方向

如果继续演进，优先级较高的方向包括：

1. 将 `ConcurrentDataChunkChannel` 更名为更贴近实现的名字，例如 `BlockingDataChunkChannel`
2. 把元数据通道命名和数据通道命名进一步统一，减少“queue/channel 混用”造成的理解成本
3. 视需要继续推进真正的 coroutine-native stage 流转
4. 为 `soratransport pack/unpack` 增加和 `fasttar` 一致的可选压缩 CLI
5. 增加更系统的性能基准文档，例如大目录、小文件、大文件三类场景的吞吐记录
