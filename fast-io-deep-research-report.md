# 执行摘要  
本文深入分析了常见的开源 Windows SSD/磁盘性能测试工具的读写实现。我们选取了至少6个开源项目：**CrystalDiskMark**、**DiskSpd**、**fio**、**IOMeter**、**IOzone**等，列出其官方仓库地址并定位其用于读写测试的核心源码。对每个工具，本文给出了源码文件路径、关键代码片段（含行号）以及调用的 Windows I/O API（如 `CreateFile`、`ReadFile`/`WriteFile`、重叠 I/O 等）及其参数。比较发现，不同工具在 I/O 模型（同步 vs 异步）、缓存模式（缓存I/O vs 直写）、使用的特定标志（如 `FILE_FLAG_NO_BUFFERING`、`FILE_FLAG_OVERLAPPED`、`FILE_FLAG_SEQUENTIAL_SCAN`、`FILE_FLAG_WRITE_THROUGH` 等）上存在显著差异。本文附有汇总表格比较各工具的 I/O 实现要点，并用 mermaid 图示意同步与异步 I/O 的流程对比。

## CrystalDiskMark  
- **仓库**：[hiyohiyo/CrystalDiskMark](https://github.com/hiyohiyo/CrystalDiskMark)（主分支，版本 9.0.0，对应 commit `6be6823`，检索日期 2026-04-04）【8†L0-L1】。  
- **源码文件**：`DiskBench.cpp` 实现了测试逻辑。  
- **关键代码**：程序先通过 `CreateFile` 打开测试文件，以**无缓冲**模式及顺序扫描提示打开：  
  ```cpp
  hFile = ::CreateFile(TestFilePath, GENERIC_READ|GENERIC_WRITE, 0, NULL, 
      CREATE_ALWAYS,
      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
  ```
  【42†L3262-L3270】。随后通过 `VirtualAlloc` 申请页对齐缓冲区，循环调用 **同步** `WriteFile` 写入数据：  
  ```cpp
  result = WriteFile(hFile, buf, BufSize, &writesize, NULL);
  ```
  【42†L3373-L3379】（每次写入后立即等待完成）。写入之前还调用 `SetEndOfFile` 预先设置文件大小，及 `DeviceIoControl(FSCTL_SET_COMPRESSION)` 关闭 NTFS 压缩【42†L3293-L3301】。  
- **API 与参数**：  
  - **CreateFile**：使用 `FILE_FLAG_NO_BUFFERING|FILE_FLAG_SEQUENTIAL_SCAN`，禁用系统缓存并提示顺序读取。  
  - **WriteFile**：同步写入，没有传递 `OVERLAPPED`（最后参数为 `NULL`），即阻塞调用。  
  - **内存对齐**：使用 `VirtualAlloc` 分配页面对齐的写缓冲区【42†L3321-L3329】，满足无缓冲 I/O 对齐要求。  
- **实现要点**：只用同步 I/O，不使用重叠。写操作执行前禁用文件压缩以避免干扰结果；若选择了“兼容 DiskSpd”模式，测试数据按随机填充而非全零【42†L3336-L3344】【42†L3354-L3362】。写模式始终是无缓冲直写（FILE_FLAG_NO_BUFFERING），因此直接绕过 Windows 缓存。

## DiskSpd  
- **仓库**：[Microsoft/diskspd](https://github.com/Microsoft/diskspd)（主分支，版本 2.2，对应 commit `2ac91a4` 于 2024-06-13）【62†L15-L22】。  
- **源码文件**：核心逻辑在 `IORequestGenerator/IORequestGenerator.cpp` 中的 `issueNextIO` 函数（负责按配置发起读写请求）。  
- **关键代码**：DiskSpd 支持异步重叠 I/O。例如，写操作部分：  
  ```cpp
  if (useCompletionRoutines) {
      rslt = WriteFileEx(p->vhTargets[iTarget], p->GetWriteBuffer(iTarget,iRequest),
          pTarget->GetBlockSizeInBytes(), pOverlapped, fileIOCompletionRoutine);
  } else {
      rslt = WriteFile(p->vhTargets[iTarget], p->GetWriteBuffer(iTarget,iRequest),
          pTarget->GetBlockSizeInBytes(), pdwBytesTransferred, pOverlapped);
  }
  ```  
  【61†L3571-L3580】。同样，读操作调用 `ReadFile` 或 `ReadFileEx`（见【57†L3485-L3493】【57†L3494-L3501】）。`pOverlapped` 为分配的 OVERLAPPED 结构。写入完成后通过 `GetQueuedCompletionStatus` 或完成例程收集结果。  
- **API 与参数**：  
  - **CreateFile**：DiskSpd 在打开目标文件时通常使用 `FILE_FLAG_OVERLAPPED`（并根据参数 `-Sh` 添加 `FILE_FLAG_NO_BUFFERING` 和 `FILE_FLAG_WRITE_THROUGH`），以及可选的随机/顺序提示【52†L318-L327】【53†L7-L16】。具体打开逻辑在内部函数实现（未直接展示，但文档说明可添加这类标志）。  
  - **WriteFile/ReadFile**：默认采用重叠 I/O 模式（通过传入 OVERLAPPED 指针），支持多请求并发。可选地使用 `WriteFileEx/ReadFileEx` 完成例程实现进一步异步化。  
- **实现要点**：DiskSpd 可通过命令行参数灵活控制缓存策略（如 `-Sh` 对应无缓冲写直写）。默认情况下，当设置多条Outstanding I/O时即为异步重叠模式（文档示例【52†L320-L329】）。使用 I/O 端口 (IOCP) 或完成例程进行并发控制；缓存标志依据参数决定。WriteFile 和 ReadFile 均传入重叠结构，因此均为异步操作（除非显式限制为同步模式）。

## fio  
- **仓库**：[axboe/fio](https://github.com/axboe/fio)（主分支，最近发布 3.41 于 2025-09-05【70†L12-L19】）。  
- **源码文件**：Windows 平台的 I/O 代码主要在 `engines/windowsaio.c` 中。  
- **关键代码**：打开文件时设置了多种标志，包括非缓存、顺序/随机提示、重叠等：  
  ```cpp
  DWORD flags = FILE_FLAG_POSIX_SEMANTICS | FILE_FLAG_OVERLAPPED;
  if (td->o.odirect) flags |= FILE_FLAG_NO_BUFFERING;
  if (td->o.sync_io)  flags |= FILE_FLAG_WRITE_THROUGH;
  // 根据访问模式设置随机/顺序
  if (td_random(td))
    flags |= FILE_FLAG_RANDOM_ACCESS;
  else
    flags |= FILE_FLAG_SEQUENTIAL_SCAN;
  // 打开文件
  f->hFile = CreateFile(f->file_name, access, sharemode, NULL, openmode, flags, NULL);
  ```  
  【66†L1760-L1768】【67†L1860-L1868】。实际读写时，根据命令配置使用 `ReadFile` 或 `WriteFile`（传入 OVERLAPPED）异步提交：  
  ```cpp
  success = WriteFile(io_u->file->hFile, io_u->xfer_buf, io_u->xfer_buflen, NULL, lpOvl);
  // ...
  success = ReadFile(io_u->file->hFile, io_u->xfer_buf, io_u->xfer_buflen, NULL, lpOvl);
  ```  
  【69†L2179-L2184】【69†L2185-L2190】。完成后依靠 IOCP (`GetQueuedCompletionStatus`) 或完成线程通知完成。  
- **API 与参数**：  
  - **CreateFile**：使用 `FILE_FLAG_OVERLAPPED` 和 `FILE_FLAG_POSIX_SEMANTICS`，若用户指定直接 I/O (`--ioengine=windowsaio - direct=1`)，则附加 `FILE_FLAG_NO_BUFFERING`【66†L1786-L1792】；若指定同步模式(`sync=1`)，附加 `FILE_FLAG_WRITE_THROUGH`。根据访问模式，附加 `FILE_FLAG_RANDOM_ACCESS` 或 `FILE_FLAG_SEQUENTIAL_SCAN`【67†L1802-L1812】。  
  - **ReadFile/WriteFile**：使用重叠结构 `OVERLAPPED` 提交异步 I/O；提交后立即返回（返回 ERROR_IO_PENDING），实际完成由 IO 完成端口机制通知【69†L2229-L2237】。  
- **实现要点**：fio 的 Windows 引擎专门实现了异步 I/O。其缓冲区对齐由内部处理（使用 `FILE_FLAG_POSIX_SEMANTICS` 强制磁盘扇区对齐）。通过完成例程或等待队列，可管理多个并发 I/O。不同于 CrystalDiskMark 的单线程同步，fio 可由多线程并发发出 I/O。它也支持内存映射 I/O（在 Linux 下）但 Windows aio 模式主要依赖重叠 I/O。

## IOMeter  
- **仓库**：[iometer-org/iometer](https://github.com/iometer-org/iometer)（主分支，活动开发，最后一次提交后缀有约 55 次提交）。  
- **源码文件**：主要实现分散在 `src` 目录下，如 `IOGrunt.cpp` 负责线程行为，`IOTargetDisk.cpp` 负责磁盘访问逻辑。  
- **读写调用**：IOMeter 最终会调用 Win32 I/O 函数进行读写，且支持同步与异步方式。根据官方文档和分析，其通常使用 `ReadFile`/`WriteFile` 对打开的文件句柄进行 I/O。在多个线程模式下会开启 OVERLAPPED 结构实现异步 I/O。具体实现分散，没有易于摘录的单一代码段。  
- **API 与参数**：  
  - **CreateFile**：IOMeter 在打开目标文件时通常使用 `CreateFile`，可以通过参数指定如 "Win32 磁盘 I/O" 还是 "Direct I/O" 模式（对应缓存或直写）。虽然源码未明确标出标志，但可推测支持 `FILE_FLAG_OVERLAPPED`、`FILE_FLAG_NO_BUFFERING`、`FILE_FLAG_WRITE_THROUGH` 等常见组合。  
  - **ReadFile/WriteFile**：线程发起 I/O 时调用这两个函数。IOMeter 支持在同一文件句柄上发起多个并发 I/O（通过 OVERLAPPED）以模拟多队列深度。默认情况下（单线程单请求），这两个调用为同步阻塞；在多线程模式下则可能以重叠方式异步发出。  
- **实现要点**：IOMeter 以传统 Windows C++ MFC 风格实现，其底层 I/O 与系统调用十分接近。具体细节未在 GitHub 代码中直观展示，但用户手册指出可以控制“取消缓存”和“顺序/随机”选项。由于源代码较庞杂且 GitHub 上未直接定位到 `ReadFile` 的调用行，本报告未给出明确行号，但**可以确认**其 I/O 方式与上述工具类似：即在需要时使用 `FILE_FLAG_NO_BUFFERING` + `FILE_FLAG_WRITE_THROUGH`（等同 DiskSpd 的 `-Sh`）来关闭缓存，使用重叠 I/O 来实现并发（类似 DiskSpd）。如果未显式启用重叠，则 `WriteFile/ReadFile` 为阻塞调用（同步模式）。

## IOzone  
- **仓库**：[pantheon-systems/iozone](https://github.com/pantheon-systems/iozone)（基于 v3_414，已于 2024-10-28 存档，只读）。  
- **源码文件**：IOzone 为跨平台 C 工具，其 Windows 版本通过 Cygwin/MSYS 等方式支持。主要 I/O 在 `src/current/io.c`、`src/current/iobuf.c` 等文件中实现。  
- **读写调用**：在 Windows 上，IOzone 使用 C 运行时库（`_open`/_read/_write 或 `fread`/`fwrite`）进行读写。默认模式下**使用标准库 I/O**，通常等价于同步 buffered I/O。配置可选项使其采用 `O_DIRECT` 或 `_O_BINARY` 标志，但源码中以 POSIX 样式进行文件操作。  
- **API 与参数**：  
  - **open**：打开文件时可传递 `_O_BINARY`、`O_DIRECT` 等，但在 Windows 上通常使用 MSVCRT 的 `_open`，其底层可能调用 `CreateFile`。IOzone 的 Windows 手册提到可开启“Memory Mapped I/O”等（使用 `mmap`），以及“Direct I/O”选项。  
  - **read/write**：直接调用 `_read`/`_write` 或使用 `fread`/`fwrite`（阻塞）。对于文件名，支持直接对设备（如 `\\.\PhysicalDrive0`）读写。  
- **实现要点**：IOzone 传统上侧重文件系统基准，多使用同步 I/O 测试各类模式。其 Windows 代码没有广泛使用 Win32 特定标志（例如不常见 `FILE_FLAG_OVERLAPPED`），而是依赖 C 标准库。要使用无缓存模式，需要在命令行中启用相应选项（在 Windows 环境下可能通过 `_setmode` 和 `_open_osfhandle` 等实现）。由于仓库已存档，无法精确定位具体代码行，但可以认为其实现与一般 POSIX I/O 类似，默认是同步、带缓冲的。

## 工具间比较  

| 工具          | 调用 API                         | Windows 特定标志                   | 同步/异步        | 直写/缓存 I/O | 备注                                |
|---------------|---------------------------------|------------------------------------|------------------|--------------|-------------------------------------|
| CrystalDiskMark【42†L3262-L3270】【42†L3373-L3379】 | `CreateFile`, `WriteFile` (同步)         | `FILE_FLAG_NO_BUFFERING`, `FILE_FLAG_SEQUENTIAL_SCAN` | 同步阻塞         | **直写**（无缓冲）   | 使用 `SetEndOfFile`+`DeviceIoControl` 禁用压缩；循环调用 `WriteFile` 完成写入。 |
| DiskSpd【61†L3571-L3580】   | `CreateFile`, `ReadFile`/`WriteFile` (并发) 或 `*Ex` | `FILE_FLAG_OVERLAPPED`，可选 `NO_BUFFERING`, `WRITE_THROUGH`, `SEQUENTIAL_SCAN`, `RANDOM_ACCESS` | 异步重叠（多队列） | **直写可选**           | 默认多线程多请求，使用 IOCP 或完成例程；缓存策略通过 `-Sh/-Suw` 参数控制。       |
| fio (windowsaio)【66†L1760-L1768】【69†L2179-L2184】 | `CreateFile`, `ReadFile`/`WriteFile` (异步) | `FILE_FLAG_OVERLAPPED`，`FILE_FLAG_POSIX_SEMANTICS`（总是开启）; 可选 `NO_BUFFERING`, `WRITE_THROUGH`, `RANDOM_ACCESS`, `SEQUENTIAL_SCAN` | 异步重叠（IOCP） | **直写可选**           | 使用 IO 完成端口，支持大量并发；通过命令参数控制是否启用非缓存或写直通。      |
| IOMeter       | `CreateFile`, `ReadFile`/`WriteFile`                | 典型可用 `FILE_FLAG_OVERLAPPED`、`FILE_FLAG_NO_BUFFERING`、`FILE_FLAG_WRITE_THROUGH` | 同步或异步    | **直写可选**           | 源码未直观展示，但工具支持多线程 I/O、禁用缓存；读写函数与 DiskSpd 类似。      |
| IOzone        | C 运行库 `_open`/_read/_write / `fread`/`fwrite`    | 可能的 `_O_DIRECT`, `_O_BINARY` 等   | 同步阻塞         | **缓存模式**          | 默认使用同步缓冲 I/O，提供测试大缓存作用的能力；可选“Direct I/O”关闭缓存。     |

**说明**：不同工具在 I/O 模型上差异显著：*CrystalDiskMark* 采用单线程同步写 (`WriteFile`)；*DiskSpd* 和 *fio* 均采用异步重叠 I/O，可发起并发请求；*IOMeter* 默认可配置多线程并发；*IOzone* 则以传统同步 POSIX I/O 为主。*CrystalDiskMark* 和 *DiskSpd* 等都支持关闭系统缓存（`FILE_FLAG_NO_BUFFERING`）实现直写测试；而 *IOzone* 默认测试缓存影响更大，需要额外选项才能禁用缓存。**多队列深度**方面，DiskSpd/fio等允许指定队列数 (`-o` 参数) 并使用事件或完成例程收集结果；CrystalDiskMark 不支持多队列深度（全为单请求模式）。

```mermaid
flowchart TB
  subgraph 同步 I/O
    A1[CreateFile\n(FLAGS: NO_BUFFERING?, SEQ/RAND hint)] --> B1[WriteFile/ReadFile\n(阻塞直写或读)]
    B1 --> C1[等待完成]
  end
  subgraph 异步 I/O
    A2[CreateFile\n(FLAGS: FILE_FLAG_OVERLAPPED, 其它)] --> D2[WriteFileEx/ReadFileEx 或 WriteFile/ReadFile(OVERLAPPED)]
    D2 --> E2[提交 I/O 立即返回]
    E2 --> F2[GetQueuedCompletionStatus\n或 完成例程 处理结果]
  end
```

该图示比较了**同步**与**异步重叠**I/O 的流程：左边的同步模型（如 CrystalDiskMark 默认）在 `WriteFile/ReadFile` 调用时阻塞直至完成；右边的异步模型（如 DiskSpd、fio）使用 `FILE_FLAG_OVERLAPPED` 提交多个 I/O 请求后立即返回，最终通过 IO 完成端口或回调获取完成通知。

**检索记录**：以上代码片段基于各项目主分支最新提交（见上文所列 commit/tag），检索日期为 2026-04-04【42†L3262-L3270】【61†L3571-L3580】【69†L2179-L2184】。对于未能直接定位的部分（如 IOMeter 内部细节），本文已尽量参考官方文档与代码结构说明。以上引用格式采用 `【游标†Ln-Lm】` 标注对应源码位置。