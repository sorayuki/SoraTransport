这是一份针对极速局域网文件夹传输系统与 fasttar 工具的当前架构设计文档。

当前实现采用“协程控制平面 + 线程化流水线数据面”的方案。控制连接、监听和 socket 建立时使用 Boost.Asio 协程；目录扫描、文件打开、顺序读取、tar 打包、压缩等重负载阶段通过线程、阻塞队列和阻塞通道连接。设计目标仍然是高吞吐、低峰值内存和清晰的阶段边界；当前普通文件 payload 采用顺序 `CreateFileMappingW + MapViewOfFile + PrefetchVirtualMemory` 路径，但仍然避免乱序分块读取。

## 1. 总体架构

系统被拆分为若干明确的阶段，每个阶段只承担一种职责。阶段之间通过有界队列或有界通道连接，从而保留背压能力。

### 1.1 发送端 / 本地打包数据流

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
(3) TarPacker
    │  顺序消费已经打开的 FileReader，驱动 libarchive
    ▼
[TarQueue]
    │
    ├── (4a) RawTarWriter
    │
    └── (4b) ZstdCompressor
              │
              ▼
         FileByteSink / SocketByteSink
```

### 1.2 接收端 / 本地解包数据流

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

- 多线程递归遍历目录
- 生成 `FileMeta`
- 将相对路径统一转换为 UTF-8 generic path，供 tar 条目名使用
- 不负责打开文件，也不负责读取文件内容

### 2.2 FileReaderOpener

这是当前实现里的关键新增阶段。

- 输入是 `FileMeta`
- 输出是 `OpenedFileReader`
- 内部复用 reader 线程池并发执行 `FileReader::open()`
- 通过序号重排保证输出顺序和输入顺序一致
- 通过有界 `OpenedQueue` 限制“已经 open 但尚未消费”的 reader 数量，避免一次性打开过多文件句柄

这个阶段的存在是为了解决两个实际问题：

- `CreateFileW` / `CreateFileMappingW` 本身是阻塞操作，适合从主打包线程中拆出去
- 直接并发 open 太多文件会触发句柄压力，因此必须配合有界队列和顺序输出

### 2.3 FileReader

当前 `FileReader` 的设计是“一个实例只负责一个文件、只允许顺序读取”。

- 构造时绑定文件路径、文件大小和用户态缓冲区大小
- `open()` 只打开一次文件
- `read_next_chunk()` 每次顺序读取一个块
- 析构或 `close()` 时关闭文件

当前实现对普通文件使用只读 file mapping，并把每个顺序 chunk 的 `MapViewOfFile + PrefetchVirtualMemory` 派发到 reader 线程池执行。消费侧仍按调用方给定的块大小顺序前进，不做乱序分块读取。

### 2.4 TarPacker

- 输入是 `OpenedFileReader`
- 为每个文件写 tar header
- 对普通文件顺序调用 `read_next_chunk()`
- 将数据交给 libarchive，再通过 callback 推送到 tar 数据通道

当前 `TarPacker` 不再自己维护乱序读的 reorder buffer，也不再自己派发单文件的并发读请求。文件打开和文件读取都已经前移并收敛到更简单的模型。

### 2.5 ZstdCompressor / ZstdDecompressor

- `ZstdCompressor` 负责将 tar 数据流转成 zstd 数据流
- `ZstdDecompressor` 负责反向恢复 tar 数据流
- 压缩阶段继续运行在独立的 compression 线程池上

### 2.6 TarUnpacker

- 通过 libarchive 读取 tar 数据流
- 负责目录创建、条目恢复和磁盘写入
- 当前直接使用 `archive_write_disk`，而不是自定义文件写回流水线

## 3. 并发模型

当前实现采用三类并发资源：

- scanner 线程：用于目录遍历
- reader 线程池：用于并发执行 `FileReaderOpener` 的 open 操作
- compression 线程池：用于 zstd 压缩

网络侧额外使用 Boost.Asio 的 `io_context` 驱动协程控制流，但数据面主体仍然是线程和阻塞同步原语。

## 4. 背压与资源控制

当前实现的资源控制主要依赖几个有界结构：

- `BoundedQueue<FileMeta>`：限制待处理元数据积压
- `BoundedQueue<OpenedFileReader>`：限制同时打开的文件数
- `ConcurrentDataChunkChannel`：限制 tar 数据在压缩或写出前的积压

其中 `OpenedQueue` 的引入是当前设计最重要的资源控制点。它把并发 open 文件的吞吐收益和文件句柄上限之间的矛盾收敛在一个可控范围内。

## 5. 实现取舍

### 5.1 保留的目标

- 模块边界清晰
- 阶段之间有背压
- 对大目录和大量小文件场景保持稳定
- 网络和本地打包共用同一套数据面

### 5.2 当前刻意放弃的复杂度

- 不再对单文件做乱序并发的 mmap 读调度
- 不再对单文件做乱序并发分块读取
- 不再使用实验性 Boost `concurrent_channel`
- 不再强行把所有阶段写成 coroutine-native stage

这些简化换来的是更稳定的生命周期管理、更少的资源泄漏风险，以及更容易诊断的问题边界。

## 6. 命令行与工具目标

当前工程包含三个可执行目标：

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
  - 可选 `--read-files`，通过当前真实流水线验证打开与顺序读取性能

## 7. 后续演进方向

如果后续继续演进，优先级较高的方向包括：

1. 把 `FileReaderOpener` 的并发深度和 opened queue 深度做成更明确的运行时配置项
2. 为 `soratransport pack/unpack` 提供和 `fasttar` 一致的压缩模式开关
3. 如有明确收益，再评估是否为大文件重新引入平台特定快路径，但应保持“单 reader 单文件”的生命周期模型
4. 补充更系统的性能基准记录，覆盖大目录、小文件和大文件场景
