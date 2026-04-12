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
- [src/detail2/config.hpp](src/detail2/config.hpp)
  - `PipelineTuning`
  - `make_pipeline_tuning()`
- [src/detail2/infra.hpp](src/detail2/infra.hpp)
  - `SemaphoreCor`
  - `TaskExecutor`
- [src/detail2/filesystem.hpp](src/detail2/filesystem.hpp)
  - `FileTraverser`
  - `FileOpener`
  - `FilePrefetcher`
  - `OpenedFile`
- [src/detail2/tar.hpp](src/detail2/tar.hpp)
  - `detail2::TarPacker`
- [src/detail2/zstd.hpp](src/detail2/zstd.hpp)
  - `detail2::ZstdCompressor`
- [src/detail2/orchestration.cpp](src/detail2/orchestration.cpp)
  - 顶层 `pack` / `listen` 编排
  - 保留旧 `unpack` / `receive` 路径的搬运实现
- [src/detail2/gui_runtime.hpp](src/detail2/gui_runtime.hpp)
  - GUI 发送页对外状态接口
- [src/detail2/gui_runtime.cpp](src/detail2/gui_runtime.cpp)
  - GUI 发送页状态机复用旧交互逻辑，但内部发送流水线切到 `detail2`
- [src/detail2/writer.hpp](src/detail2/writer.hpp)
  - `detail2::BufferedFileWriter`
- [src/detail2/stream.hpp](src/detail2/stream.hpp)
  - `detail2::QueueWriter`
  - `detail2::RawTarReader`
  - `detail2::ZstdDecompressor`
  - `detail2::TarUnpacker`

当前 `detail2` 目录已经进入第二阶段：`pack`、`listen` 和 GUI 发送页的实际发送流水线都已切到 `detail2`，而 `unpack` / `receive` 与落盘路径暂时仍由 `src/detail/` 提供。

## 3. 当前数据流

### 3.1 本地打包

`pack_directory_to_file()` 当前流程：

1. `detail2::FileTraverser` 以 BFS 方式扫描目录并输出 `TraversalEntry`
2. `detail2::FileOpener` 对普通文件并发 open，并把 open guard 绑定到 `OpenedFile`
3. `detail2::FilePrefetcher` 在字节预算信号量允许范围内对已打开文件执行初始预读
4. `detail2::TarPacker` 顺序消费 reader，生成 tar 数据流
4. 根据模式选择：
  - raw tar：`QueueWriter`
  - zstd：`detail2::ZstdCompressor`
5. 最终写入 `FileByteSink`

### 3.2 本地解包

`unpack_file_to_directory()` 当前流程：

1. 根据模式选择 `RawTarReader` 或 `ZstdDecompressor`
2. 输出 tar 数据流到 `BoundedQueue<DataChunk>`
3. `TarUnpacker` 解析条目后，目录与元数据仍由 libarchive 协助恢复，普通文件内容则交给 `ExtractWriteScheduler -> ExtractFileWriter` 批量写回磁盘

### 3.3 网络发送

`listen_directory()` 的控制流由 Boost.Asio 协程监听并 `accept()` WebSocket 连接，但数据面和本地打包一致：

1. 监听端先完成 TCP accept 和 WebSocket server handshake
2. 发送一条文本 JSON 控制帧 `transport_begin`
3. 扫描目录
4. 并发打开普通文件
5. `detail2::FilePrefetcher` 预读
6. `detail2::TarPacker` 打 tar
7. `detail2::ZstdCompressor` 压缩
8. 通过 `SocketByteSink` 以 WebSocket 二进制消息发送连续字节流
9. 发送一条文本 JSON 控制帧 `transport_end`
10. 若调用方提供 `TransferProgress`，后台线程会同步已处理字节数与文件数

### 3.3.1 GUI 发送

GUI 发送页不再直接调用 `listen_directory()`，而是使用 `GuiSendServer` 做一个更贴合交互的会话层：

1. 启动窗口时立即绑定一个固定监听端口，并生成可复制的 `soratrans://` 链接
2. 接收端先连入该端口并完成 WebSocket handshake，发送页界面切换为拖放框
3. 用户一次拖放可提交多个文件或目录
4. 每次 drop 都会发送一组 `transport_begin -> 二进制数据 -> transport_end`
5. `GuiSendServer` 复用现有 `DirScanner -> FileReaderPrefetcher -> TarPacker -> ZstdCompressor -> SocketByteSink` 流水线，把这些根路径按给定顺序发送出去
5. `GuiSendServer` 当前已经改为复用 `detail2::FileTraverser -> detail2::FileOpener -> detail2::FilePrefetcher -> detail2::TarPacker -> detail2::ZstdCompressor -> SocketByteSink` 流水线，把这些根路径按给定顺序发送出去
6. 单次拖放完成后连接保持不变，继续等待下一次拖放；空闲期间由服务端定时发出 WebSocket ping 保活

### 3.4 网络接收

`receive_directory()` 当前流程：

1. 通过 Boost.Asio 协程主动 `connect()` 到远端，并完成 WebSocket client handshake
2. 等待文本 JSON 控制帧 `transport_begin`
3. `SocketByteSource` 把后续 WebSocket 二进制消息拼成连续字节流
4. 收到文本 JSON 控制帧 `transport_end` 时，把本次读取视为一次压缩流结束
5. `ZstdDecompressor` 解压为 tar 数据流
6. `TarUnpacker` 落盘
7. CLI 模式下一次传输完成后退出；GUI 模式会继续等待后续 `transport_begin`
8. 若调用方提供 `TransferProgress`，后台线程会同步已处理字节数与文件数

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

### 5.2.1 detail2 基础设施状态

为了把新的节点逻辑从旧实现中拆开，当前已经新增一组尚未切主路径的基础设施：

- `detail2::TaskExecutor`：继续基于 Boost.Asio `post()`，但对外暴露可 `co_await` 的等待接口
- `detail2::SemaphoreCor`：提供支持 move-only guard 的协程信号量，并带有批量取消等待者的语义

这两个类当前主要用于后续重写节点时降低控制流复杂度；由于主路径尚未切换，所以运行结果仍然由旧实现决定。

### 5.2.2 detail2 文件链路状态

当前已经新增但尚未切主路径的文件链路节点包括：

- `detail2::FileTraverser`：负责多根输入、BFS 遍历、目录元数据输出以及 symlink 跳过
- `detail2::FileOpener`：负责按遍历顺序重排输出，同时把打开并发度限制交给 `SemaphoreCor`
- `detail2::FilePrefetcher`：负责在字节预算信号量控制下触发初始预读，并把预算 guard 绑定到 `OpenedFile`

这些节点当前已经进入 `soratransport_core` 构建并通过编译，但还没有接入 pack/listen 的实际执行路径。

### 5.2.3 detail2 打包侧状态

当前已经新增但尚未切主路径的打包侧节点包括：

- `detail2::TarPacker`：消费 `OpenedFile`，在真正读文件前释放预读预算 guard，并生成 tar 数据块
- `detail2::ZstdCompressor`：延续原有自适应压缩判定，同时把输出字节预算绑定到数据块生命周期
- `detail2::chunk.hpp`：用 aliasing `shared_ptr` 把预算 guard 与 `DataChunk` 生命周期绑定，供 tar/zstd 输出队列复用

这些模块的目标是为接下来的 pack/listen 切流准备一个完整但仍可逐步接线的 pack 侧实现。

### 5.2.4 detail2 顶层接线状态

当前已经完成的主路径替换包括：

- `pack_directory_to_file()` 改为调用 `detail2` 的发送侧节点
- `listen_directory()` 改为调用 `detail2` 的发送侧节点
- GUI 发送页内部的 `send_paths_to_socket()` 改为调用 `detail2` 的发送侧节点

当前仍保留旧实现的主路径包括：

- 解包阶段内部的落盘写入实现

### 5.2.5 detail2 文件写入对象状态

当前已经新增一个尚未接入解包主路径的新写入对象：

- `detail2::BufferedFileWriter`
  - 对外暴露 `write()` / `close()`
  - 使用槽位缓冲聚合写入请求
  - 通过 `TaskExecutor` 后台排空待写槽位
  - 关闭时等待后台排空完成并保持取消语义

这个对象用于承接后续 `unpack` / `receive` 的落盘重构，目前已经进入构建但尚未替换旧的解包写入实现。

### 5.2.6 detail2 接收侧包装状态

为了让顶层编排统一指向 `detail2`，当前已经新增：

- `detail2::QueueWriter`
- `detail2::RawTarReader`
- `detail2::ZstdDecompressor`
- `detail2::TarUnpacker`

这组类当前通过包装方式委托现有稳定实现，因此：

- `unpack_file_to_directory()` 与 `receive_directory()` 的顶层编排已经只引用 `detail2` 节点
- 复杂的解包与落盘内部细节暂时仍委托给旧实现，以保持结果一致

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

- `SocketByteSink` 现在封装 Boost.Beast WebSocket server/client 流，并继续采用缓冲聚合发送
- `SocketByteSink` 在 GUI 发送端支持文本 JSON 控制帧、二进制数据帧，以及空闲 ping 保活
- `SocketByteSource` 采用同步读取接口向解压阶段供数，并把多条二进制 message 重新拼成一个连续压缩字节流
- `SocketByteSource` 通过读取文本 JSON 控制帧来判定单次传输的开始与结束
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
- `fltk` 与 `nlohmann_json` 都通过 `FetchContent` 拉取，其中 `nlohmann_json` 用于解析 WebSocket 文本控制帧

## 11. 维护建议

后续维护时，优先注意以下事实：

1. 当前打包流水线已经收敛为 `DirScanner -> Prefetcher -> TarPacker -> (QueueWriter | ZstdCompressor -> QueueWriter)`。
2. `DirScanner` 已经合并了文件元数据填充和普通文件并发打开逻辑，不再有独立的 `FileReaderOpener`。
3. `InFlightReadBudget` 只绑定预读阶段，预算租约会在 `TarPacker` 真正消费对象前释放。
4. `FileByteSink` / `FileByteSource` 已不再暴露 direct/buffered 切换；若重新引入该能力，必须同步修改 CLI 语义和文档。
5. GUI、CLI、网络入口现在共用 `CancelEvent` / `TransferProgress`；后续若调整取消或状态语义，必须同步更新三者。
