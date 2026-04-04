这是一份针对 SoraTransport 与 fasttar 的当前架构设计文档。

当前实现采用“协程控制平面 + 线程化流水线数据面”的方案。网络连接仍然由 Boost.Asio 协程处理；真正的数据搬运则由阶段线程、单共享线程池、有界队列和阻塞式字节源/汇协作完成。相比更早期的设想，当前设计重点已经转向清晰的阶段边界、稳定的资源控制和易维护的 Windows I/O 实现。

## 1. 总体架构

系统拆分为若干明确阶段，每个阶段只承担一种职责。阶段之间通过有界队列连接，从而保留背压能力。

### 1.1 监听发送端 / 本地打包数据流

```text
[文件系统]
    │
    ▼
(1) DirScanner
    │  产出 FileMeta
    ▼
[MetaQueue]
    │
    ▼
(2) FileReaderOpener
    │  并发 open FileReader，但保持输入输出顺序
    ▼
[OpenedQueue]
    │
    ▼
(3) FileReaderPrefetcher
    │  在读预算允许范围内执行初始预读
    ▼
[PrefetchedQueue]
    │
    ▼
(4) TarPacker
    │  顺序消费 FileReader，驱动 libarchive
    ▼
[TarQueue]
    │
    ├── (5a) RawTarWriter
    │
    └── (5b) ZstdCompressor
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

- 并发递归遍历目录
- 生成 `FileMeta`
- 将相对路径统一转换为 UTF-8 generic path，供 tar 条目名使用
- 不负责打开文件，也不负责读取文件内容

### 2.2 FileReaderOpener

- 输入是 `FileMeta`
- 输出是 `OpenedFileReader`
- 在线程池中并发创建并 `open()` `FileReader`
- 通过序号重排保证输出顺序和输入顺序一致
- 不负责预算申请，也不负责初始预读

### 2.3 FileReaderPrefetcher

- 输入是已经 open 的 `OpenedFileReader`
- 输出是已经完成初始预读的 `OpenedFileReader`
- 使用 `InFlightReadBudget` 控制预读占用的总内存预算
- 预算会跟随对象进入 `prefetched_queue (p/t)`，并在对象被 `TarPacker` 取出时立即释放

### 2.4 FileReader

当前 `FileReader` 的设计是“一个实例只负责一个文件、对外只允许顺序消费”。

- 构造时绑定文件路径、文件大小、chunk 大小和 I/O 模式
- `open()` 只打开一次文件
- `start_prefetch(max_bytes)` 触发初始预读窗口
- `read_next_chunk()` 每次按顺序返回一个块
- 析构或 `close()` 时关闭文件

实现特征：

- 使用 Windows `ReadFile + OVERLAPPED`
- 内部维护固定 8 个 read slot
- 支持 `Buffered` 与 `Direct` 模式
- Direct 模式会处理对齐和尾块读取

### 2.5 TarPacker

- 输入是 `OpenedFileReader`
- 为每个文件写 tar header
- 对普通文件顺序调用 `read_next_chunk()`
- 通过 libarchive callback 推送 tar 数据块

当前 `TarPacker` 是单线程顺序打包器，不承担额外并发调度。

### 2.6 ZstdCompressor / ZstdDecompressor

- `ZstdCompressor` 负责将 tar 数据流转成 zstd 数据流
- `ZstdCompressor` 现在是独立节点：输入 `TarQueue`，输出 `ZstdQueue`
- `ZstdDecompressor` 负责反向恢复 tar 数据流
- 自适应压缩级别只绑定 `ZstdCompressor` 自身上下游队列状态
- 只有未显式指定 `-l <level>` 时才允许自适应调节；一旦用户手动指定压缩级别，就必须固定使用该级别
- 上游高压且下游低压时降档
- 下游高压或上游低压时升档
- 压缩工作会投递到共享线程池执行

### 2.7 TarUnpacker

- 通过 libarchive 读取 tar 数据流
- 负责目录创建、条目恢复和磁盘写入
- 当前直接使用 `archive_write_disk`

## 3. 并发模型

当前实现采用两类并发资源：

- 外围阶段线程：使用 `std::jthread` 驱动各阶段
- 内部共享执行器：`RuntimeExecutors` 封装单个 Boost.Asio thread pool

网络侧额外使用 Boost.Asio `io_context` 驱动 `listen -> accept` 和 `receive -> connect` 协程，但数据面主体仍然是线程和阻塞同步原语。

默认情况下，共享线程池大小等于硬件核心数。

## 4. 背压与资源控制

当前实现的资源控制主要依赖几个有界结构：

- `BoundedQueue<FileMeta>`：限制待处理元数据积压
- `BoundedQueue<OpenedFileReader>`：限制已打开文件数
- `BoundedQueue<OpenedFileReader>`：限制已预读文件数
- `BoundedQueue<DataChunk>`：限制 tar 数据在压缩或写出前的积压

当前默认深度：

- `meta_queue = 256`
- `opened_queue = 32`
- `prefetched_queue = 64`
- `tar_queue = 16`

另一个关键控制点是 `InFlightReadBudget`：

- 限制所有初始预读总占用
- 防止多个文件同时预读时内存失控

## 5. I/O 设计

### 5.1 输入路径

- `FileByteSource` 支持 `Buffered` / `Direct`
- `FileReader` 支持 `Buffered` / `Direct`
- Direct 路径会查询设备对齐信息

### 5.2 输出路径

- `FileByteSink` 当前固定为 buffered + overlapped 写出
- 通过多写槽和 `max_in_flight_write_ops` 控制在途写请求数
- 输出侧当前不再暴露 direct write 接口

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
- 不再让 `FileReaderOpener` 同时承担 open 与预算控制

这些简化换来的是更清晰的生命周期管理、更少的耦合点和更容易诊断的问题边界。

## 7. 命令行与工具目标

当前工程包含三个可执行目标：

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
  - 可选 `--read-files`，通过真实的 open + prefetch + consume 流水线验证读取性能

其中 `-l <level>` 的语义是“显式锁定压缩级别”，不是“提供一个自适应起始值”。

## 8. 后续演进方向

如果后续继续演进，优先级较高的方向包括：

1. 把 `submit_concurrency`、队列深度和预读窗口做成更明确的运行时配置项。
2. 评估 `FileReaderPrefetcher` 是否需要更细粒度的预算生命周期，而不是“送入下游即释放”。
3. 如有明确收益，再评估是否为输出文件重新引入 Direct I/O 写路径，但应同步修改 CLI 与文档语义。
4. 补充更系统的性能基准记录，覆盖大目录、小文件和大文件场景。
