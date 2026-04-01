这是一份针对**极速局域网文件夹传输系统（兼顾独立 fasttar 工具）**的 C++20 架构设计文档。

本系统采用基于 C++20 协程 (Coroutines) 与 Boost.Asio 的流水线（Pipeline）架构。通过严格的模块解耦、内存池复用和背压（Backpressure）控制，实现在极低内存占用下打满 SSD I/O 与网络带宽。

🚀 极速局域网传输与打包系统架构设计 (C++20)
1. 系统宏观架构与流水线设计
整个系统被拆分为独立的数据处理阶段（Stage），阶段之间通过**有界并发通道（Bounded Concurrent Channels）**连接。这种设计天然支持背压：当网络或压缩模块处理较慢时，通道填满会反向挂起（Suspend）上游的读取和扫描操作，彻底杜绝处理 10GB 以上大文件时发生 OOM。

1.1 模块流转图 (Data Flow)
【发送端 (Sender) / 打包端 (Packer)】

Plaintext
[文件系统] 
   │
   ▼
(1) DirScanner (多线程目录遍历) -> 产出 FileMeta
   │
   ▼ [Bounded Channel: MetaQueue]
   │
(2) TarPacker 引擎 (流控与打包核心)
   │   ├── 发起异步读取请求 -> (3) FileReader (内存映射/AIO 线程池)
   │   └── 接收有序 DataChunk
   │
   ▼ 调用 libarchive (通过自定义 Write Callback) -> 产出 TarChunk
   │
   ▼ [Bounded Channel: TarQueue]
   │
(4) ZstdCompressor (流式压缩) -> 产出 ZstdChunk
   │
   ▼ [Bounded Channel: NetQueue]
   │
(5) NetSender (Boost.Asio TCP/RDMA) -> 发送至局域网
注：单独编译 fasttar 工具时，可按需组装两种本地模式：
- 无压缩模式：组装 (1)->(2)->(3)，并将 (2) 的输出直接通过 FileWriter 写入本地 .tar 文件。
- 压缩模式：组装 (1)->(2)->(3)->(4)，将 ZstdCompressor 的输出写入本地 .tar.zst 文件；解包时对称地先解压再喂给 TarUnpacker。

2. 核心模块与接口设计
所有模块均通过 C++20 Concept 约束，内部实现主要依赖 boost::asio::awaitable 实现无锁的异步流转。

2.0 基础组件：内存池与数据块 (Buffer Pool)
为了避免频繁的 new/delete 带来系统开销和内存碎片，定义全局共享内存池。

大小分级：提供 4KB (小文件/元数据) 和 16MB (大文件块) 两个级别的 Slab Pool。

结构体定义：

C++
struct DataChunk {
    std::shared_ptr<uint8_t> data; // 指向 BufferPool 的智能指针，自带 custom deleter
    size_t length;
    uint64_t offset;               // 数据在文件中的偏移量
    bool is_eof;                   // 是否是文件的最后一块
};

struct FileMeta {
    std::filesystem::path path;
    std::filesystem::file_status status;
    uintmax_t size;
    std::string relative_path_in_tar;
};
2.1 目录扫描模块 (DirScanner)
功能：多线程递归遍历目录，极速获取文件元数据（通过 std::filesystem::directory_iterator 或系统底层的 getdents64 封装）。

设计细节：使用基于工作窃取（Work-Stealing）的线程池。主协程遇到子目录时，将其作为独立 Task 抛入线程池，文件元数据通过 boost::asio::experimental::concurrent_channel 发送给下游。

C++
class IDirScanner {
public:
    virtual ~IDirScanner() = default;
    // 异步启动扫描，并将结果压入 channel
    virtual boost::asio::awaitable<void> scan(
        std::filesystem::path root_dir, 
        boost::asio::experimental::concurrent_channel<void(boost::system::error_code, FileMeta)>& out_queue) = 0;
};
2.2 极速文件读取模块 (FileReader)
功能：榨干 SSD I/O 性能。针对 10GB 大文件，使用分块内存映射 (Chunked mmap) 技术。

加速策略：

利用 boost::interprocess::file_mapping 和 mapped_region。

平台特性下沉：封装一个跨平台的 Prefetcher。在 Windows 上调用 PrefetchVirtualMemory；在 Linux 上调用 posix_madvise(MADV_WILLNEED)。虽然优先使用 Boost，但 Boost 尚未提供直接的 Prefetch 语义，因此在这里使用条件编译隔离宏调用，这是达到“尽可能快”所必须的妥协。

每次映射 16MB 或 64MB。

C++
class IFileReader {
public:
    virtual ~IFileReader() = default;
    // 异步读取文件的指定区块
    virtual boost::asio::awaitable<DataChunk> read_chunk_async(
        const std::filesystem::path& path, 
        uint64_t offset, 
        size_t length) = 0;
};
2.3 归档打包引擎 (TarPacker)
功能：这是解决并发读取与 libarchive 单线程顺序写入冲突的核心引擎。

工作机制：

从 MetaQueue 获取下一个待处理的 FileMeta。

调用 archive_write_header。

如果是小文件，直接一次性发起 read_chunk_async；如果是 10GB 大文件，将其切分为多个 16MB 块。

多线程 I/O 乱序读取，单线程顺序组装：允许 FileReader 并发读取大文件的多个块，但 TarPacker 内部使用一个滑动窗口（Sliding Window / Reorder Buffer），严格按 offset 顺序调用 archive_write_data。

libarchive 的输出通过 archive_write_open_memory 或注册 write_callback，将打包好的 TarChunk 压入下游通道。

C++
class ITarPacker {
public:
    // 将 Scanner 管道和 Reader 绑定，流式输出 Tar 数据块
    virtual boost::asio::awaitable<void> pack_stream(
        boost::asio::experimental::concurrent_channel<void(boost::system::error_code, FileMeta)>& in_meta,
        std::shared_ptr<IFileReader> reader,
        boost::asio::experimental::concurrent_channel<void(boost::system::error_code, DataChunk)>& out_tar) = 0;
};
2.4 流式压缩模块 (ZstdCompressor / Decompressor)
功能：集成 libzstd。

设计细节：不使用完整的按文件压缩，而是使用 ZSTD_CStream（Streaming API）。从 TarQueue 读取任意大小的块，送入 ZSTD，产生高度压缩的数据块，压入网络通道。

独立线程：压缩是 CPU 密集型任务，分配给 Boost.Asio 线程池中特定的线程执行，不阻塞 I/O 协程。

2.5 网络模块 (NetSender / NetReceiver)
功能：利用 boost::asio::ip::tcp::socket 异步发送/接收数据流。

设计细节：直接调用 boost::asio::async_write。得益于前置的 Bounded Channel，如果网络卡顿，Channel 会满，自动引发一连串的协程挂起，直到读取端停止映射新文件。

3. 线程与并发模型设计 (Concurrency Model)
为了兼顾隔离性与资源利用率，系统启动时初始化一个全局的 boost::asio::thread_pool，但在投递任务时按职责进行执行器（Executor）隔离（strand 或独立的 io_context）：

I/O 调度协程 (1 线程)：负责驱动全局的 TarPacker 状态机，它是非常轻量的逻辑控制。

I/O Worker 线程池 (如 4-8 线程)：专属 FileReader，执行底层的 mmap 和 PrefetchVirtualMemory，这些操作可能发生缺页中断（Page Fault）导致线程阻塞，因此必须与逻辑线程隔离。

计算 Worker 线程池 (依赖核心数)：专属 ZstdCompressor，全速跑满 CPU。

网络线程 (1-2 线程)：处理 Socket 异步收发。

4. 单独编译 fasttar 工具的设计支持
本架构天然支持被剥离为单机工具：

复用模块：直接引入 DirScanner、FileReader 和 TarPacker。

替换末端：编写一个 LocalFileWriter 类，它同样监听 TarPacker 输出的 Channel。

LocalFileWriter 实现：直接使用 boost::asio::stream_file (Boost 1.77+) 或大块异步文件写入。无压缩模式下将 TarChunk 原封不动写入本地 .tar 文件；启用压缩模式时，将 ZstdChunk 写入本地 .tar.zst 文件。

CLI 约定：fasttar 提供显式开关选择是否压缩。`fasttar pack --zstd <source-dir> <output.tar.zst>` 生成压缩归档，`fasttar pack --no-compress <source-dir> <output.tar>` 生成原始 tar；解包命令对称支持 `--zstd` 与 `--no-compress`。未显式指定时，可按输出或输入文件扩展名自动推断模式。

5. 内存消耗推演与大文件 (10GB+) 处理保障
当遇到 10GB 大文件时：

TarPacker 发现文件大小为 10GB，将其拆分为 640 个 16MB 的读取任务（Offset: 0, 16M, 32M...）。

向 FileReader 发起前 4 个并发异步请求（控制并发深度，如 Depth=4）。

FileReader 申请 4 块 16MB 内存，调用 mmap 并执行 PrefetchVirtualMemory，促使操作系统利用 DMA 将数据从 SSD 搬入物理内存。

某一块就绪后返回给 TarPacker。TarPacker 按序将 16MB 数据喂给 libarchive，然后立即释放这 16MB 的 shared_ptr 回收至内存池。

继续派发下一个 16MB 请求。
结论：对单个 10GB 文件的处理，无论文件多大，内存峰值始终被严格限制在 并发深度(4) × ChunkSize(16MB) + Pipeline缓冲 ≈ 100MB 级别。

6. 接收端 (Receiver) 逆向流转设计
接收端的数据流是完全逆向且对称的：

NetReceiver: 从 Socket 不断 async_read 二进制流，压入 ZstdQueue。

ZstdDecompressor: 初始化 ZSTD_DStream，解压还原出 TarChunk，压入 TarQueue。

TarUnpacker:

调用 archive_read_open_memory 或注册读回调，每次从 TarQueue 拿一块数据喂给 libarchive。

通过 archive_read_next_header 解析出文件路径与大小。

遇到目录则创建目录（std::filesystem::create_directories）。

遇到文件则驱动 FileWriter 模块。

FileWriter: 针对 10GB 大文件同样使用 mmap 写入。预先 std::filesystem::resize_file 分配 10GB 空间，随后分块 mmap，将解压出的数据 memcpy 进去，交由操作系统异步刷盘（Page Cache）。

7. 异常处理与恢复机制 (Error Handling)
所有跨模块的 Channel 定义为 (boost::system::error_code, T)。

当 FileReader 遇到权限拒绝（Permission Denied）时，返回相应的 error_code。

TarPacker 截获错误，调用 libarchive 写入相应的错误日志或跳过该文件。

当网络断开时，底层的 async_write 抛出/返回断开错误，该错误顺着 Channel 向上游逆向传播，引发协程链式取消（Cancellation），优雅释放所有内存和文件句柄。