# SoraTransport Implementation Notes

本文档记录当前代码库的实际实现状态，作为 [Design.md](Design.md) 的落地说明。内容以“当前代码真实如何运行”为准。

## 1. 当前可执行目标

当前工程构建三个可执行程序：

- `soratransport`
  - `pack [-d|-b] [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <output.tar.zst>`
  - `unpack [-d|-b] [-r <MiB>] [-w <count>] <input.tar.zst> <destination-dir>`
  - `listen [-r <MiB>] [-l <level>] <source-dir> <port>`
  - `receive <host> <port> <destination-dir>`
- `fasttar`
  - `pack [-d|-b] [-z|-n] [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <output.tar|output.tar.zst>`
  - `unpack [-d|-b] [-z|-n] [-r <MiB>] [-w <count>] <input.tar|input.tar.zst> <destination-dir>`
- `fs_benchmark`
  - 默认扫描 `D:/dev/boost_1_90_0/dist`
  - 使用 `--read-files` 时走 open + prefetch + 顺序消费链路

## 2. 代码组织

### 2.1 公开入口

- [src/core.hpp](/D:/dev/soratransport/src/core.hpp)
- [src/core.cpp](/D:/dev/soratransport/src/core.cpp)
- [src/main.cpp](/D:/dev/soratransport/src/main.cpp)
- [src/fasttar.cpp](/D:/dev/soratransport/src/fasttar.cpp)
- [src/fs_benchmark.cpp](/D:/dev/soratransport/src/fs_benchmark.cpp)

### 2.2 细分模块

- [src/detail/types.hpp](/D:/dev/soratransport/src/detail/types.hpp)
  - `DataChunk`
  - `FileMeta`
  - `CompressionMode`
  - `BoundedQueue<T>`
- [src/detail/runtime.hpp](/D:/dev/soratransport/src/detail/runtime.hpp)
  - `BufferPool`
  - `RuntimeExecutors`
- [src/detail/runtime.cpp](/D:/dev/soratransport/src/detail/runtime.cpp)
  - `make_runtime_config()`
  - `PipelineState`
  - `BufferPool` 实现
- [src/detail/io.hpp](/D:/dev/soratransport/src/detail/io.hpp)
  - 字节源/汇接口声明
- [src/detail/io.cpp](/D:/dev/soratransport/src/detail/io.cpp)
  - `FileByteSource`
  - `FileByteSink`
  - `SocketByteSource`
  - `SocketByteSink`
- [src/detail/filesystem.cpp](/D:/dev/soratransport/src/detail/filesystem.cpp)
  - `DirScanner`
  - `InFlightReadBudget`
  - `FileReader`
  - `FileReaderOpener`
  - `FileReaderPrefetcher`
- [src/detail/tar.cpp](/D:/dev/soratransport/src/detail/tar.cpp)
  - `TarPacker`
  - `TarUnpacker`
- [src/detail/zstd.cpp](/D:/dev/soratransport/src/detail/zstd.cpp)
  - `ZstdCompressor`
  - `ZstdDecompressor`
  - `RawTarWriter`
  - `RawTarReader`
- [src/detail/orchestration.cpp](/D:/dev/soratransport/src/detail/orchestration.cpp)
  - 顶层本地/网络编排
- [src/detail/cli.cpp](/D:/dev/soratransport/src/detail/cli.cpp)
  - CLI 解析与 usage

## 3. 当前数据流

### 3.1 本地打包

`pack_directory_to_file()` 当前流程：

1. `DirScanner` 产出 `FileMeta`
2. `FileReaderOpener` 并发 `open()`，顺序输出 `OpenedFileReader`
3. `FileReaderPrefetcher` 在预算允许范围内对已打开文件执行初始预读
4. `TarPacker` 顺序消费 reader，生成 tar 数据流
5. 根据模式选择：
   - `RawTarWriter`
   - `ZstdCompressor`
6. 最终写入 `FileByteSink`

### 3.2 本地解包

`unpack_file_to_directory()` 当前流程：

1. 根据模式选择 `RawTarReader` 或 `ZstdDecompressor`
2. 输出 tar 数据流到 `BoundedQueue<DataChunk>`
3. `TarUnpacker` 通过 libarchive 写回磁盘

### 3.3 网络发送

`listen_directory()` 的控制流由 Boost.Asio 协程监听并 `accept()` 连接，但数据面和本地打包一致：

1. 扫描目录
2. 打开文件
3. 预读
4. 打 tar
5. zstd 压缩
6. 通过 `SocketByteSink` 发送

### 3.4 网络接收

`receive_directory()` 当前流程：

1. 通过 Boost.Asio 协程主动 `connect()` 到远端
2. `SocketByteSource` 读取字节流
3. `ZstdDecompressor` 解压为 tar 数据流
4. `TarUnpacker` 落盘

## 4. 文件读取实现现状

### 4.1 FileReader

当前 `FileReader` 是单文件、顺序消费语义的 Windows overlapped 读取器：

- 一个 `FileReader` 只绑定一个文件
- `open()` 只执行一次
- `read_next_chunk()` 始终按 offset 顺序消费
- 内部维护最多 8 个 overlapped read slot
- 支持 `Buffered` 和 `Direct` 两种读模式

### 4.2 FileReaderOpener

这是当前文件读取链上的第一阶段：

- 在线程池中并发创建 `FileReader` 并执行 `open()`
- 通过 `std::map<序号, future>` 做顺序重排
- 只负责 open，不负责预算和预读

### 4.3 FileReaderPrefetcher

这是当前文件读取链上的第二阶段：

- 输入是已经 open 的 `OpenedFileReader`
- 根据 `InFlightReadBudget` 申请预算
- 调用 `reader.start_prefetch(max_bytes)`
- 将完成初始预读的 reader 推给 `TarPacker`
- 在对象送入下游后立即释放本阶段预算

这里需要特别注意：

- 当前预算只保证“启动预读窗口时不会无限放大内存占用”
- 预算并不持续绑定到 `TarPacker` 的整个消费周期

### 4.4 缓冲区策略

当前约定：

- 主流水线 chunk 大小为 4 MiB
- `fs_benchmark` 的读块大小为 8 MiB
- Direct I/O 会按设备对齐要求修正单次请求大小

## 5. 并发与同步原语

### 5.1 线程模型

当前实现采用两层并发：

- 外围阶段线程：`std::jthread`
- 内部共享执行器：单个 Boost.Asio thread pool

### 5.2 RuntimeExecutors

`RuntimeExecutors` 当前只维护一个共享线程池：

- `post()` 用于提交需要并发执行的工作
- 默认线程数等于硬件核心数

目录扫描、文件 open 和 zstd 压缩共享这一执行器。

### 5.3 队列

当前主要使用项目内的 `BoundedQueue<T>`：

- `meta_queue`
- `opened_queue`
- `prefetched_queue`
- `tar_queue`

所有队列都是有界的，背压通过阻塞 `push()` 自然传播。

## 6. 压缩与归档实现

### 6.1 TarPacker

当前 `TarPacker`：

- 顺序消费 `OpenedFileReader`
- 通过 libarchive 写 tar header 和 payload
- 不承担额外并发调度职责

### 6.2 TarUnpacker

仍然使用 libarchive 的 `archive_write_disk` 写回磁盘。

### 6.3 Zstd

- 压缩使用 `ZSTD_compressStream2()`
- 解压使用 `ZSTD_decompressStream()`
- 会尝试配置 `ZSTD_c_nbWorkers`
- 自适应压缩级别仍然可用

## 7. I/O 路径现状

### 7.1 FileByteSource

- 支持 `Buffered` / `Direct`
- Direct 模式下会查询扇区对齐信息

### 7.2 FileByteSink

- 当前接口不再接收 `FileIoMode`
- 固定使用 buffered + overlapped 写出
- 通过 `max_in_flight_write_ops` 控制在途异步写数量

## 8. CLI 行为

### 8.1 soratransport

`soratransport` 当前仍固定使用 zstd 格式：

- `pack` 输出 `.tar.zst`
- `unpack` 输入 `.tar.zst`
- `listen` 监听端口并在连接建立后发送目录
- `receive` 主动连接到远端并接收目录

### 8.2 fasttar

`fasttar` 支持：

- `-z`
- `-n`
- 根据输入/输出扩展名自动推断

推断规则：

- `.tar` => `CompressionMode::None`
- `.tar.zst` / `.tzst` / `.zst` => `CompressionMode::Zstd`

## 9. 当前默认参数

当前关键常量：

- `kPipelineChunkSize = 4 MiB`
- `kMetaQueueDepth = 256`
- `kOpenedQueueDepth = 32`
- `kPrefetchQueueDepth = 64`
- `kTarQueueDepth = 16`
- `kOverlappedReadQueueDepth = 8`
- `kTargetInFlightReadBytes = 128 MiB`
- `kDefaultMaxInFlightWriteOps = 1`

## 10. 构建状态

[CMakeLists.txt](/D:/dev/soratransport/CMakeLists.txt) 当前定义：

- `soratransport_core`
- `soratransport`
- `fasttar`
- `fs_benchmark`

## 11. 维护建议

后续维护时，优先注意以下事实：

1. 当前打包流水线已经是 `open` 与 `prefetch` 分离的四阶段结构。
2. `InFlightReadBudget` 只绑定预读阶段，不再和 `TarPacker` 消费周期绑定。
3. `FileByteSink` 现在是固定 buffered 写接口，若重新引入 direct write，应同步修改 CLI 语义和文档。
4. `RuntimeExecutors` 已经收敛为单共享线程池，后续若再拆池，需要同步更新设计文档和进度观测说明。
5. 网络命令语义已经变更为 `listen = accept`、`receive = connect`，后续若继续调整命令名或参数形态，必须同步更新公开 API 和 CLI 帮助文本。
