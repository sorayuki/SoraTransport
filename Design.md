这是一份针对 SoraTransport 与 fasttar 的当前架构设计文档。

当前实现采用“协程控制平面 + 线程化流水线数据面 + 可取消运行时”的方案。网络连接仍然由 Boost.Asio 协程处理，但协议层已经切到 Boost.Beast WebSocket：二进制消息承载 zstd 压缩 tar 字节流，文本消息承载 JSON 控制事件。真正的数据搬运则由阶段线程、单共享线程池、有界队列和阻塞式字节源/汇协作完成。相比更早期的设想，当前设计重点已经转向清晰的阶段边界、稳定的资源控制、统一的 Windows overlapped I/O 路径，以及 GUI/CLI 共用的一套取消与进度接口。

## 1. 总体架构

系统拆分为若干明确阶段，每个阶段只承担一种职责。阶段之间通过有界队列连接，从而保留背压能力。

### 1.1 监听发送端 / 本地打包数据流

```text
[文件系统]
    │
    ▼
(1) DirScanner
    │  递归遍历目录，填充 FileMeta，并发打开普通文件
    ▼
[OpenedQueue]
    │
    ▼
(2) FileReaderPrefetcher
    │  在读预算允许范围内执行初始预读
    ▼
[PrefetchedQueue]
    │
    ▼
(3) TarPacker
    │  顺序消费 FileReader，驱动 libarchive
    ▼
[TarQueue]
    │
    ├── (4a) QueueWriter
    │        │  仅 raw tar 路径使用（当前主要是 fasttar -n）
    │        ▼
    │   FileByteSink
    │
    └── (4b) ZstdCompressor
              │  独立压缩节点
              ▼
          [ZstdQueue]
               │
               ▼
          QueueWriter
               │
               ▼
          FileByteSink / SocketByteSink
```

### 1.2 主动接收端 / 本地解包数据流

```text
FileByteSource / SocketByteSource
    │
    ├── RawTarReader
    │
    └── ZstdDecompressor
            │
            ▼
        [TarQueue]
            │
            ▼
        TarUnpacker
```

## 2. 当前阶段职责

### 2.1 DirScanner

- 递归遍历目录，并保留根目录条目
- 也可以按给定顺序处理多个独立根路径，允许 GUI 一次拖放多个文件或目录
- 为所有条目填充 `FileMeta`
- 将相对路径统一转换为 UTF-8 generic path，供 tar 条目名使用
- 普通文件会在线程池中并发打开，并以 `OpenedFileReader` 的形式顺序输出
- 目录等非普通文件直接输出元数据；symlink 当前仍跳过
- 同时采集 Windows 时间戳和文件属性，供 tar 元数据写回

### 2.2 FileReaderPrefetcher

- 输入是已经 open 的 `OpenedFileReader`
- 输出是已经完成初始预读的 `OpenedFileReader`
- 使用 `InFlightReadBudget` 控制预读占用的总内存预算
- 预算会跟随对象进入 `prefetched_queue (p/t)`，并在对象被 `TarPacker` 取出时立即释放

### 2.3 FileReader

当前 `FileReader` 的设计是“一个实例只负责一个文件、对外只允许顺序消费”。

- 构造时直接绑定文件路径、文件大小、chunk 大小和已打开的文件句柄
- `start_prefetch(max_bytes)` 触发初始预读窗口
- `read_next_chunk()` 每次按顺序返回一个块
- 析构时关闭文件

实现特征：

- 使用 Windows `ReadFile + OVERLAPPED`
- 内部维护固定 8 个 read slot
- 具体 overlapped 读取细节封装在 `OverlappedFileReader`

### 2.4 TarPacker

- 输入是 `OpenedFileReader`
- 为每个文件写 tar header
- 对普通文件顺序调用 `read_next_chunk()`
- 通过 libarchive callback 推送 tar 数据块
- 在真正写 tar payload 前会先释放预读预算租约

当前 `TarPacker` 是单线程顺序打包器，不承担额外并发调度。

### 2.5 ZstdCompressor / ZstdDecompressor

- `ZstdCompressor` 负责将 tar 数据流转成 zstd 数据流
- `ZstdCompressor` 现在是独立节点：输入 `TarQueue`，输出 `ZstdQueue`
- `ZstdDecompressor` 负责反向恢复 tar 数据流
- 自适应压缩级别只绑定 `ZstdCompressor` 自身上下游队列状态
- 只有未显式指定 `-l <level>` 时才允许自适应调节；一旦用户手动指定压缩级别，就必须固定使用该级别
- 上游高压且下游低压时降档
- 下游高压或上游低压时升档
- 压缩工作会投递到共享线程池执行

### 2.6 TarUnpacker

- 通过 libarchive 读取 tar 数据流
- 负责目录创建、条目恢复和磁盘写入
- 当前使用自定义的 `ExtractWriteScheduler` / `ExtractFileWriter` 负责普通文件落盘，而不是把文件内容直接交给 `archive_write_disk`
- 解包写盘与输出文件写盘保持同样的思路：优先聚合到批量缓冲，达到容量阈值或时间阈值后再提交写入，避免大量零碎小写请求
- 解包时会校验条目路径必须是相对路径，防止越界写入目标目录

## 3. 并发模型

当前实现采用两类并发资源：

- 外围阶段线程：使用 `std::jthread` 驱动各阶段
- 内部共享执行器：`RuntimeExecutors` 封装单个 Boost.Asio thread pool

网络侧额外使用 Boost.Asio `io_context` 驱动 `listen -> accept` 和 `receive -> connect` 协程，但数据面主体仍然是线程和阻塞同步原语。

默认情况下，共享线程池大小取 `min(hardware_concurrency, 12)`；文件打开并发度则按 `worker_threads * 4` 计算，并再限制到最多 48。

### 3.1 取消与进度

- `CancelEvent` 作为统一取消信号源
- `BoundedQueue`、`InFlightReadBudget`、文件/套接字 I/O 对象都会监听取消信号
- GUI 和网络入口通过 `std::stop_token -> CancelEvent` 桥接来中断运行中的传输
- `TransferProgress` 为 GUI 与网络模式提供统一的状态、字节数和文件数快照

## 4. 背压与资源控制

当前实现的资源控制主要依赖几个有界结构：

- `BoundedQueue<OpenedFileReader>`：限制已打开文件数
- `BoundedQueue<OpenedFileReader>`：限制已预读文件数
- `BoundedQueue<DataChunk>`：限制 tar 数据在压缩或写出前的积压
- `BoundedQueue<DataChunk>`：在压缩路径上再限制 zstd 数据积压

当前默认深度：

- `opened_queue = max(32, file_open_concurrency)`
- `prefetched_queue = 64`
- `tar_queue = 16`
- `zstd_queue = 16`

另一个关键控制点是 `InFlightReadBudget`：

- 限制所有初始预读总占用
- 防止多个文件同时预读时内存失控
- 默认预算是 `96 MiB`
- 默认输出并发写请求数是 `3`

## 5. I/O 设计

### 5.1 输入路径

- `FileReader` 与 `FileByteSource` 当前都走统一的 Windows overlapped 文件读取路径
- 输入侧不再对 CLI 暴露 `Buffered` / `Direct` 模式切换
- 解包文件输入通过 `FileByteSource -> OverlappedFileReader` 顺序消费

### 5.2 输出路径

- `FileByteSink` 当前固定为 buffered + overlapped 写出
- 写盘缓冲采用“容量阈值 + 时间阈值”策略：默认聚合到 4 MiB，或在首字节进入缓冲后等待约 100ms 仍未写满时主动提交
- 通过多写槽和 `max_in_flight_write_ops` 控制在途写请求数
- 解包落盘也采用同样的批量写思路：小文件同步写和常规 overlapped 写路径都会先做聚合，再按容量或时间阈值落盘
- 输出侧当前不再暴露 direct write 接口

### 5.3 网络路径

- `SocketByteSink` / `SocketByteSource` 也接入统一取消语义
- 网络协议在 TCP 连接之上使用 Boost.Beast WebSocket
- 二进制消息只承载压缩 tar 字节流；message 边界不再定义一次传输的开始和结束
- 文本消息使用 JSON 控制事件；当前约定 `{"type":"event","event_id":"transport_begin"}` 与 `{"type":"event","event_id":"transport_end"}`
- 监听发送端在连接空闲时会主动发出 WebSocket ping，避免 NAT 映射因长时间空闲被回收
- 网络发送端固定走 zstd 压缩路径；raw tar 只在本地 `fasttar -n` 路径使用

## 6. 实现取舍

### 6.1 保留的目标

- 模块边界清晰
- 阶段之间有背压
- 对大目录和大量小文件场景保持稳定
- 网络和本地打包共用同一套数据面

### 6.2 当前刻意放弃的复杂度

- 不再使用 mmap 快路径
- 不再对单文件做乱序并发分块读取
- 不再为 reader/compression/scanner 维护多个独立执行器
- 不再保留独立的 `FileReaderOpener` 阶段
- 不再对外暴露 Direct I/O / Buffered I/O 选择

这些简化换来的是更清晰的生命周期管理、更少的耦合点和更容易诊断的问题边界。

## 7. 命令行与工具目标

当前工程包含四个可执行目标：

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
  - 可选 `--read-files`，通过真实的 open + prefetch + consume 流水线验证读取性能
- `soratransport_app`
  - 基于 FLTK 的 Windows GUI
    - 主界面固定为“发送 / 接收”两个 tab
    - 发送页启动后立即绑定监听端口并展示可复制的 `soratrans://` 链接
        - 当接收端先连入发送页后，界面切换为拖放框；一次拖放可提交多个文件或目录，并触发一个 `transport_begin -> 二进制数据 -> transport_end` 传输片段
        - 同一个 GUI 发送连接会在一次 drop 完成后继续保持，等待下一次拖放；只有断开或取消时才重新回到“等待接收端”
        - 接收页提供发送者链接输入框，按回车或点“连接”后才发起接收；GUI 接收连接建立后会持续等待后续传输片段
    - 启动时如果剪贴板里有可识别的 `soratrans://` 链接，则默认切到接收页并预填输入框；否则默认切到发送页
    - GUI 不再接受命令行参数来决定模式

其中 `-l <level>` 的语义是“显式锁定压缩级别”，不是“提供一个自适应起始值”。

## 8. 后续演进方向

如果后续继续演进，优先级较高的方向包括：

1. 把 `submit_concurrency`、队列深度和预读窗口做成更明确的运行时配置项。
2. 评估 `FileReaderPrefetcher` 是否需要更细粒度的预算生命周期，而不是“送入下游即释放”。
3. 如有明确收益，再评估是否为输出文件重新引入 Direct I/O 写路径，但应同步修改 CLI 与文档语义。
4. 补充更系统的性能基准记录，覆盖大目录、小文件和大文件场景。
