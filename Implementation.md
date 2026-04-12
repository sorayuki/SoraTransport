# SoraTransport Implementation Notes

本文档记录当前代码库的实际实现状态，作为 [Design.md](Design.md) 的落地说明。内容以“当前代码真实如何运行”为准。

## 1. 当前可执行目标

当前工程构建四个可执行程序：

- `soratransport`
  - `pack [-r <MiB>] [-w <count>] [-l <level>] <source-dir> <output.tar.zst>`
  - `unpack [-r <MiB>] [-w <count>] <input.tar.zst> <destination-dir>`
  - `listen [-r <MiB>] [-l <level>] [--log-adaptive] <source-dir> <port>`
  - `receive <host> <port> <destination-dir>`
- `fasttar`
  - `pack [-z|-n] [-r <MiB>] [-w <count>] [-l <level>] [--log-adaptive] <source-dir> <output.tar|output.tar.zst>`
  - `unpack [-z|-n] [-r <MiB>] [-w <count>] <input.tar|input.tar.zst> <destination-dir>`
- `fs_benchmark`
  - 默认扫描 `D:/dev/boost_1_90_0/dist`
  - 使用 `--read-files` 时走 open + prefetch + 顺序消费链路
- `soratransport_app`
  - 基于 FLTK 的 Windows GUI
  - 主窗口固定包含“发送 / 接收”两个 tab，不再根据命令行参数选择模式
  - 发送页通过 `GuiSendServer` 先绑定监听端口，接收端连入后等待一次拖放来触发发送
  - 接收页通过输入框 + “连接”按钮显式启动 `receive_directory()`
  - 两个 tab 分别维护自己的 `TransferProgress` 状态，并共享底部统计区域展示当前选中 tab 的进度

## 2. 代码组织

### 2.1 公开入口

- [src/core.hpp](src/core.hpp)
- [src/core.cpp](src/core.cpp)
- [src/main.cpp](src/main.cpp)
- [src/fasttar.cpp](src/fasttar.cpp)
- [src/fs_benchmark.cpp](src/fs_benchmark.cpp)
- [src/gui_app.cpp](src/gui_app.cpp)

### 2.2 细分模块

- [src/detail/types.hpp](src/detail/types.hpp)
  - `DataChunk`
  - `FileMeta`
  - `CompressionMode`
  - `BoundedQueue<T>`
- [src/detail/runtime.hpp](src/detail/runtime.hpp)
  - `BufferPool`
  - `RuntimeExecutors`
- [src/detail/runtime.cpp](src/detail/runtime.cpp)
  - `make_runtime_config()`
  - `PipelineState`
  - `BufferPool` 实现
- [src/detail/io.hpp](src/detail/io.hpp)
  - 字节源/汇接口声明
- [src/detail/io.cpp](src/detail/io.cpp)
  - `OverlappedFileReader`
  - `FileByteSource`
  - `FileByteSink`
  - `SocketByteSource`
  - `SocketByteSink`
- [src/detail/filesystem.cpp](src/detail/filesystem.cpp)
  - `DirScanner`
  - `InFlightReadBudget`
  - `FileReader`
  - `FileReaderPrefetcher`
- [src/detail/tar.cpp](src/detail/tar.cpp)
  - `TarPacker`
  - `TarUnpacker`
- [src/detail/zstd.cpp](src/detail/zstd.cpp)
  - `ZstdCompressor`
  - `ZstdDecompressor`
  - `RawTarReader`
- [src/detail/orchestration.cpp](src/detail/orchestration.cpp)
  - 顶层本地/网络编排
- [src/detail/cli.cpp](src/detail/cli.cpp)
  - CLI 解析与 usage
- [src/detail/gui_runtime.hpp](src/detail/gui_runtime.hpp)
  - GUI 发送页的监听、接收端接入、拖放发送状态机
- [src/detail/gui_runtime.cpp](src/detail/gui_runtime.cpp)
  - GUI 发送页复用现有流水线，将多根路径打包并推送到网络套接字
- [src/detail/windows_helpers.hpp](src/detail/windows_helpers.hpp)
  - Windows 错误文本和 GUI 辅助函数

## 3. 当前数据流

### 3.1 本地打包

`pack_directory_to_file()` 当前流程：

1. `DirScanner` 扫描目录、填充 `FileMeta`，并对普通文件并发打开句柄后顺序输出 `OpenedFileReader`
2. `FileReaderPrefetcher` 在预算允许范围内对已打开文件执行初始预读
3. `TarPacker` 顺序消费 reader，生成 tar 数据流
4. 根据模式选择：
   - raw tar：`QueueWriter`
   - zstd：`ZstdCompressor`
5. 最终写入 `FileByteSink`

### 3.2 本地解包

`unpack_file_to_directory()` 当前流程：

1. 根据模式选择 `RawTarReader` 或 `ZstdDecompressor`
2. 输出 tar 数据流到 `BoundedQueue<DataChunk>`
3. `TarUnpacker` 解析条目后，目录与元数据仍由 libarchive 协助恢复，普通文件内容则交给 `ExtractWriteScheduler -> ExtractFileWriter` 批量写回磁盘

### 3.3 网络发送

`listen_directory()` 的控制流由 Boost.Asio 协程监听并 `accept()` 连接，但数据面和本地打包一致：

1. 扫描目录
2. 并发打开普通文件
3. 预读
4. 打 tar
5. zstd 压缩
6. 通过 `SocketByteSink` 发送
7. 若调用方提供 `TransferProgress`，后台线程会同步已处理字节数与文件数

### 3.3.1 GUI 发送

GUI 发送页不再直接调用 `listen_directory()`，而是使用 `GuiSendServer` 做一个更贴合交互的会话层：

1. 启动窗口时立即绑定一个固定监听端口，并生成可复制的 `soratrans://` 链接
2. 接收端先连入该端口，发送页界面切换为拖放框
3. 用户一次拖放可提交多个文件或目录
4. `GuiSendServer` 复用现有 `DirScanner -> FileReaderPrefetcher -> TarPacker -> ZstdCompressor -> SocketByteSink` 流水线，把这些根路径按给定顺序发送出去
5. 单次拖放完成后关闭当前连接，再回到等待下一接收端的状态

### 3.4 网络接收

`receive_directory()` 当前流程：

1. 通过 Boost.Asio 协程主动 `connect()` 到远端
2. `SocketByteSource` 读取字节流
3. `ZstdDecompressor` 解压为 tar 数据流
4. `TarUnpacker` 落盘
5. 若调用方提供 `TransferProgress`，后台线程会同步已处理字节数与文件数

## 4. 文件读取实现现状

### 4.1 DirScanner

当前 `DirScanner` 已经承担“扫描 + 填充元数据 + 并发打开普通文件”的职责：

- 递归遍历目录，并保留根目录条目
- 也支持一次接收多个根路径，供 GUI 发送页直接打包多个文件或目录
- 生成 `FileMeta`，其中包含 UTF-8 tar 相对路径
- 通过 `std::filesystem` 和 `GetFileInformationByHandleEx()` 填充大小、时间戳、Windows 文件属性
- 普通文件会在线程池中并发打开，再按扫描顺序输出 `OpenedFileReader`
- symlink 当前直接跳过，不进入 tar 流

### 4.2 FileReader / OverlappedFileReader

当前 `FileReader` 是单文件、顺序消费语义的读取器外观，底层实际由 `OverlappedFileReader` 执行 Windows overlapped 读取：

- 一个 `FileReader` 只绑定一个文件
- 构造时直接接收已打开的文件句柄
- `read_next_chunk()` 始终按 offset 顺序消费
- 内部维护最多 8 个 overlapped read slot
- `start_prefetch()` 用于预热初始读取窗口
- 支持取消信号并在需要时调用 `CancelIoEx()`

### 4.3 FileReaderPrefetcher

这是当前文件读取链上的第二阶段：

- 输入是已经 open 的 `OpenedFileReader`
- 根据 `InFlightReadBudget` 申请预算
- 调用 `reader.start_prefetch(max_bytes)`
- 将完成初始预读的 reader 推给 `TarPacker`
- 预算租约会随 `OpenedFileReader` 一起进入 `p/t` 队列
- 对象一旦被 `TarPacker` 从 `p/t` 取出，就立刻释放预算

这里需要特别注意：

- 当前预算直接约束 `p/t` 队列中的预读常驻内存
- `TarPacker` 消费速度变慢时，背压会通过预算和队列容量共同限制新的预读对象进入 `p/t`
- `TarPacker` 拿到对象后的文件读取与打包过程不再继续占用这部分预算

### 4.4 缓冲区策略

当前约定：

- 主流水线 chunk 大小为 4 MiB
- `fs_benchmark` 的读块大小为 8 MiB
- 读侧固定使用 overlapped 文件读取，不再暴露 `FileIoMode`

## 5. 并发与同步原语

### 5.1 线程模型

当前实现采用两层并发：

- 外围阶段线程：`std::jthread`
- 内部共享执行器：单个 Boost.Asio thread pool

### 5.2 RuntimeExecutors

`RuntimeExecutors` 当前只维护一个共享线程池：

- `post()` 用于提交需要并发执行的工作
- 默认线程数是 `min(hardware_concurrency, 12)`

目录扫描、文件 open 和 zstd 压缩共享这一执行器；默认文件打开并发度是 `worker_threads * 4`，上限 48。

### 5.3 队列

当前主要使用项目内的 `BoundedQueue<T>`：

- `opened_queue`
- `prefetched_queue`
- `tar_queue`
- `zstd_queue`

所有队列都是有界的，背压通过阻塞 `push()` 自然传播。

### 5.4 取消机制

- `CancelEvent` 是当前统一的取消信号源
- `BoundedQueue`、`InFlightReadBudget`、`FileReader`、`FileByteSink`、`SocketByteSink`、`SocketByteSource` 都可监听取消
- `listen_directory()` / `receive_directory()` 会把 `std::stop_token` 转换为 `CancelEvent`
- 取消后会抛出 `CancelledError`，并主动关闭队列或取消挂起 I/O

### 5.5 进度机制

- `TransferProgress` 记录累计字节数、文件数和状态文本
- 网络发送/接收任务会启动后台同步线程，把内部计数刷新到 `TransferProgress`
- GUI 直接消费 `TransferProgressSnapshot`

## 6. 压缩与归档实现

### 6.1 TarPacker

当前 `TarPacker`：

- 顺序消费 `OpenedFileReader`
- 通过 libarchive 写 tar header 和 payload
- 在写入 tar 前释放 `OpenedFileReader` 携带的预读预算租约
- 不承担额外并发调度职责

### 6.2 TarUnpacker

当前 `TarUnpacker` 会把目录、权限和时间戳恢复与普通文件内容写入拆开处理：

- 只接受相对路径条目
- 拒绝 `..` 越界路径
- 当前跳过 symlink 条目
- 目录创建和目录元数据恢复由 `CreatedDirectoryIndex` / `DirectoryMetadataFinalizer` 协调
- 普通文件内容通过 `ExtractWriteScheduler` 分发到后台任务；每个文件内部由 `ExtractFileWriter` 执行 buffered + overlapped 写出
- 小文件同步写路径 `write_small_extracted_file()` 也不再按 chunk 直写，而是先聚合到缓冲区后再落盘

### 6.3 Zstd

- 压缩使用 `ZSTD_compressStream2()`
- 解压使用 `ZSTD_decompressStream()`
- 会尝试配置 `ZSTD_c_nbWorkers`
- 自适应压缩级别只在未指定 `-l <level>` 时启用
- 一旦 CLI 解析到 `-l <level>`，运行时就把该值视为用户锁定值，压缩线程不得再自动升降档
- `ZstdCompressor` 现在作为独立节点运行：`TarQueue -> ZstdQueue -> QueueWriter -> sink`
- 当前自适应策略是：
- 取上游数据时，如果发现 `t/z` 已满，就立即升档
- 向下游写数据时，如果发现 `z/s` 已满，就立即升档
- 向下游写数据时，如果发现 `z/s` 为空，就设置一个标记
- 下一次迭代取上游数据时，如果标记存在且 `t/z` 已满，就降档并清除标记
- 调节存在冷却窗口，默认每 1 秒最多允许调节一次
- 如果本次调节方向与上次相反，则取两次目标中更低的压缩级别，并把冷却窗口翻倍为 2 秒、4 秒、8 秒递增
- 如果本次调节方向与上次相同，则按当前目标调节，并把冷却窗口重置回 1 秒

## 7. I/O 路径现状

### 7.1 FileByteSource

- 只接收输入路径，不再暴露 `FileIoMode`
- 内部使用 `OverlappedFileReader` 顺序读取压缩包或 raw tar 输入
- 对解包路径来说，它是“文件 -> chunk 流”的桥接层

### 7.2 FileByteSink

- 当前接口不再接收 `FileIoMode`
- 固定使用 buffered + overlapped 写出
- 写盘策略与网络发送保持一致：优先聚合到活动缓冲，写满后提交；如果缓冲首字节进入后约 100ms 仍未写满，也会主动刷盘
- 通过 `max_in_flight_write_ops` 控制在途异步写数量
- 默认写并发是 3，而不是 1
- 解包落盘路径也采用同样的“容量阈值 + 时间阈值”聚合思路，默认批量大小同样是 4 MiB

### 7.3 Socket I/O

- `SocketByteSink` 采用缓冲聚合发送，并带有取消与停止逻辑
- `SocketByteSource` 采用同步读取接口向解压阶段供数
- 网络错误文本会通过 Windows 辅助函数转换成更稳定的 UTF-8 文本

## 8. CLI 行为

### 8.1 soratransport

`soratransport` 当前仍固定使用 zstd 格式：

- `pack` 输出 `.tar.zst`
- `unpack` 输入 `.tar.zst`
- `listen` 监听端口并在连接建立后发送目录
- `receive` 主动连接到远端并接收目录
- 不再支持 `-d` / `-b`
- `receive` 明确拒绝运行时参数

### 8.2 fasttar

`fasttar` 支持：

- `-z`
- `-n`
- 根据输入/输出扩展名自动推断

推断规则：

- `.tar` => `CompressionMode::None`
- `.tar.zst` / `.tzst` / `.zst` => `CompressionMode::Zstd`

补充约束：

- `-l <level>` 只在 `CompressionMode::Zstd` 下有意义
- `-l <level>` 表示固定压缩级别，不会触发或保留自适应调节
- `--log-adaptive` 用于打印自适应压缩调节日志

## 9. 当前默认参数

当前关键常量：

- `kPipelineChunkSize = 4 MiB`
- `kOpenedQueueDepth = 32`
- `kPrefetchQueueDepth = 64`
- `kDefaultTarQueueDepth = 16`
- `kOverlappedFileReadQueueDepth = 8`
- `kTargetInFlightReadBytes = 96 MiB`
- `kDefaultMaxInFlightWriteOps = 3`
- `kBufferedExtractWriteBatchSize = 4 MiB`
- `kMaxDefaultWorkerThreads = 12`
- `kOpenConcurrencyMultiplier = 4`
- `kMaxDefaultOpenConcurrency = 48`

## 10. 构建状态

[CMakeLists.txt](CMakeLists.txt) 当前定义：

- `soratransport_core`
- `soratransport`
- `fasttar`
- `fs_benchmark`
- `soratransport_app`
- 工程要求 `cmake_minimum_required(VERSION 4.0.0)`，并通过 vcpkg manifest 管理依赖

## 11. 维护建议

后续维护时，优先注意以下事实：

1. 当前打包流水线已经收敛为 `DirScanner -> Prefetcher -> TarPacker -> (QueueWriter | ZstdCompressor -> QueueWriter)`。
2. `DirScanner` 已经合并了文件元数据填充和普通文件并发打开逻辑，不再有独立的 `FileReaderOpener`。
3. `InFlightReadBudget` 只绑定预读阶段，预算租约会在 `TarPacker` 真正消费对象前释放。
4. `FileByteSink` / `FileByteSource` 已不再暴露 direct/buffered 切换；若重新引入该能力，必须同步修改 CLI 语义和文档。
5. GUI、CLI、网络入口现在共用 `CancelEvent` / `TransferProgress`；后续若调整取消或状态语义，必须同步更新三者。
