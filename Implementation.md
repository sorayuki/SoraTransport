# SoraTransport Implementation Notes

本文档记录当前代码库的实际实现状态，作为 [Design.md](Design.md) 的落地说明。重点不是理想目标，而是当前代码真实采用的模块边界、线程模型和数据流。

## 1. 当前可执行目标

当前工程构建三个可执行程序：

- `soratransport`
  - `pack <source-dir> <output.tar.zst>`
  - `unpack <input.tar.zst> <destination-dir>`
  - `send <source-dir> <host> <port>`
  - `receive <port> <destination-dir>`
- `fasttar`
  - `pack [--zstd|--no-compress] <source-dir> <output.tar|output.tar.zst>`
  - `unpack [--zstd|--no-compress] <input.tar|input.tar.zst> <destination-dir>`
- `fs_benchmark`
  - 默认扫描 `D:/dev/boost_1_90_0/dist`
  - 使用 `--read-files` 时走当前真实的 open + 顺序读取链路

## 2. 代码组织

### 2.1 公开入口

- [src/core.hpp](/d:/dev/soratransport/src/core.hpp)
  - 顶层 API 聚合头
- [src/core.cpp](/d:/dev/soratransport/src/core.cpp)
  - 稳定库入口编译单元
- [src/main.cpp](/d:/dev/soratransport/src/main.cpp)
  - `soratransport` 入口
- [src/fasttar.cpp](/d:/dev/soratransport/src/fasttar.cpp)
  - `fasttar` 入口
- [src/fs_benchmark.cpp](/d:/dev/soratransport/src/fs_benchmark.cpp)
  - 文件系统基准测试入口

### 2.2 细分模块

- [src/detail/types.hpp](/d:/dev/soratransport/src/detail/types.hpp)
  - `DataChunk`
  - `FileMeta`
  - `CompressionMode`
  - `BoundedQueue<T>`
  - `BlockingChannel<T>`
- [src/detail/runtime.hpp](/d:/dev/soratransport/src/detail/runtime.hpp)
  - `BufferPool`
  - `RuntimeExecutors`
- [src/detail/runtime.cpp](/d:/dev/soratransport/src/detail/runtime.cpp)
  - `make_runtime_config()`
  - `PipelineState`
  - `BufferPool` 实现
- [src/detail/io.hpp](/d:/dev/soratransport/src/detail/io.hpp)
  - 字节源/汇接口声明
- [src/detail/io.cpp](/d:/dev/soratransport/src/detail/io.cpp)
  - 文件和 socket 读写适配器
- [src/detail/filesystem.cpp](/d:/dev/soratransport/src/detail/filesystem.cpp)
  - `DirScanner`
  - `FileReader`
  - `FileReaderOpener`
- [src/detail/tar.cpp](/d:/dev/soratransport/src/detail/tar.cpp)
  - `TarPacker`
  - `TarUnpacker`
- [src/detail/zstd.cpp](/d:/dev/soratransport/src/detail/zstd.cpp)
  - `ZstdCompressor`
  - `ZstdDecompressor`
  - `RawTarWriter`
  - `RawTarReader`
- [src/detail/orchestration.cpp](/d:/dev/soratransport/src/detail/orchestration.cpp)
  - 顶层本地/网络编排
- [src/detail/cli.cpp](/d:/dev/soratransport/src/detail/cli.cpp)
  - CLI 解析与 usage

## 3. 当前数据流

### 3.1 本地打包

`pack_directory_to_file()` 当前流程：

1. `DirScanner` 产出 `FileMeta`
2. `FileReaderOpener` 并发打开文件，但保持顺序输出 `OpenedFileReader`
3. `TarPacker` 顺序消费已打开 reader，调用 libarchive 生成 tar 数据流
4. 根据模式选择：
   - `RawTarWriter`
   - `ZstdCompressor`

### 3.2 本地解包

`unpack_file_to_directory()` 当前流程：

1. 根据模式选择 `RawTarReader` 或 `ZstdDecompressor`
2. 输出 tar 数据流到 `ConcurrentDataChunkChannel`
3. `TarUnpacker` 通过 libarchive 写回磁盘

### 3.3 网络发送

`send_directory()` 的网络控制流由 Boost.Asio 协程驱动，但数据面与本地打包一致：

1. 扫描目录
2. 打开文件
3. 读取并打 tar
4. zstd 压缩
5. 通过 `SocketByteSink` 发送

### 3.4 网络接收

`receive_directory()` 当前流程：

1. `SocketByteSource` 读取字节流
2. `ZstdDecompressor` 解压为 tar 数据流
3. `TarUnpacker` 落盘

## 4. 文件读取实现现状

### 4.1 FileReader

当前 `FileReader` 是单文件、顺序读取模型：

- 一个 `FileReader` 只绑定一个文件
- `open()` 只执行一次
- `read_next_chunk()` 只按 offset 顺序前进
- 关闭逻辑集中在 `close()` 和析构里

当前普通文件读取使用 `CreateFileW + CreateFileMappingW` 建立只读映射，并通过固定数量的顺序 slot 在 reader 线程池里执行 `MapViewOfFile + PrefetchVirtualMemory`。主打包线程仍然只按 offset 顺序消费 chunk，不做乱序读取。

### 4.2 FileReaderOpener

这是当前文件读取链上的关键阶段：

- 复用 reader 线程池并发执行 `FileReader::open()`
- 通过 `std::map<序号, future>` 在阶段内做顺序重排
- 输出阶段元素 `OpenedFileReader`

为了避免一次性保持太多已打开文件，当前 opened queue 深度被显式限制为 64。

### 4.3 缓冲区策略

`FileReader` 构造时会接收一个 `buffer_size`，并将它作为顺序读取的 chunk 大小。普通文件 payload 不再额外复制到独立用户态缓冲区，而是直接以映射视图的方式暴露给后续阶段。

当前约定是：

- `TarPacker` 路径使用 `kPipelineChunkSize`
- `fs_benchmark` 路径使用 `kReadChunkSize`

也就是说，顺序文件读取、raw tar 读入和 zstd 解压产出的 chunk 现在统一对齐到 4 MiB，而不是固定写死为 1 MiB。

## 5. 并发与同步原语

### 5.1 线程池划分

- scanner 线程：目录遍历
- reader 线程池：reader open 阶段
- compression 线程池：zstd 压缩

当前 `reader_threads` 不再承担“单文件乱序分块读取”，而主要用于 `FileReaderOpener`。

### 5.2 通道实现

当前项目不依赖 Boost.Experimental 的 `concurrent_channel`。`ConcurrentDataChunkChannel` 只是一个类型别名，底层真实实现是项目内的 `BlockingChannel<DataChunk>`。

这样做的目的：

- 降低实验性依赖
- 避免事件循环线程与同步等待混用导致死锁
- 保留背压行为

## 6. 压缩与归档实现

### 6.1 TarPacker

当前 `TarPacker` 的行为已经比最初版本更简单：

- 不再自己发起并发块读
- 不再维护 reorder buffer
- 直接顺序消费已打开 `FileReader`
- 每个文件调用 libarchive 写 header 和 payload

### 6.2 TarUnpacker

仍然使用 libarchive 的 `archive_write_disk` 写回磁盘。这意味着目录创建、权限和时间戳处理继续由 libarchive 负责。

### 6.3 Zstd

- 压缩使用 `ZSTD_compressStream2()`
- 解压使用 `ZSTD_decompressStream()`
- 会尝试配置 `ZSTD_c_nbWorkers`
- 不支持多 worker 时安全降级

## 7. CLI 行为

### 7.1 soratransport

`soratransport` 当前仍固定使用 zstd 格式：

- `pack` 输出 `.tar.zst`
- `unpack` 输入 `.tar.zst`

### 7.2 fasttar

`fasttar` 支持：

- `--zstd`
- `--no-compress`
- 根据输入/输出扩展名自动推断

推断规则：

- `.tar` => `CompressionMode::None`
- `.tar.zst` / `.tzst` / `.zst` => `CompressionMode::Zstd`

## 8. 构建状态

[CMakeLists.txt](/d:/dev/soratransport/CMakeLists.txt) 当前定义：

- `soratransport_core`
- `soratransport`
- `fasttar`
- `fs_benchmark`

`soratransport_core` 当前编译单元包括：

- `src/core.cpp`
- `src/detail/runtime.cpp`
- `src/detail/io.cpp`
- `src/detail/filesystem.cpp`
- `src/detail/tar.cpp`
- `src/detail/zstd.cpp`
- `src/detail/orchestration.cpp`
- `src/detail/cli.cpp`

## 9. 当前实现和早期设想的主要差异

与最初的高阶设计相比，当前代码有以下关键变化：

1. 文件读取重新采用了顺序 `mmap + prefetch` 模型，但仍然保持“单 reader 单文件、按 offset 顺序前进”的约束
2. 新增了 `FileReaderOpener` 阶段，用于把 open 从主读取线程剥离出去
3. `TarPacker` 不再承担并发读调度逻辑
4. 通道实现已经完全项目内化，不再使用 Boost.Experimental channel
5. 当前更强调稳定性、可诊断性和资源控制，而不是追求最激进的读路径复杂度

## 10. 维护建议

后续维护时，优先注意以下事实：

1. `FileReader` 的生命周期现在和文件句柄生命周期严格绑定
2. `OpenedQueue` 深度直接决定同时打开文件数
3. `buffer_size` 与 chunk 大小绑定是当前性能调优前提之一
4. 如果继续调整 `MapViewOfFile` / `PrefetchVirtualMemory` 路径，不应破坏“单 reader 单文件”的生命周期模型
