# SoraTransport Refactor Report

## 目标

- 在 `src/detail2/` 中重写可读性更高、职责更清晰的新实现
- 保留 `src/detail/` 作为旧实现对照，不直接修改已有细节实现
- 每完成一个可独立验证的小模块，就进行一次构建验证和 git 提交

## 当前进度

### 1. 基础设施已落地

已新增：

- `src/detail2/config.hpp`
- `src/detail2/config.cpp`
- `src/detail2/infra.hpp`
- `src/detail2/infra.cpp`

本轮完成内容：

- 把本轮重构涉及的并发度、预算、槽位大小、默认压缩级别集中到 `detail2/config.*`
- 实现 `detail2::SemaphoreCor`
  - 支持一次获取多个 permit
  - 返回 move-only guard，析构自动 release
  - 支持 `cancel()` 唤醒全部等待协程并抛出 `CancelledError`
- 实现 `detail2::TaskExecutor`
  - 内部封装 Asio thread pool
  - 对外提供 `co_await` 形式的任务提交接口

### 2. 当前接线状态

- `detail2` 基础设施已经进入 `soratransport_core` 构建
- 顶层 pack/unpack/listen/receive 路径尚未切换，当前运行结果仍由 `src/detail/` 的旧节点实现产生

### 3. 文件链路节点已落地

已新增：

- `src/detail2/filesystem.hpp`
- `src/detail2/filesystem.cpp`

本轮完成内容：

- 实现 `detail2::FileTraverser`
  - 支持单根和多根输入
  - 保留 BFS 遍历与 archive root 去重逻辑
  - 继续跳过 symlink
- 实现 `detail2::FileOpener`
  - 打开普通文件时保留顺序重排语义
  - 打开并发度由 `SemaphoreCor` 控制
  - open guard 绑定到 `OpenedFile` 生命周期
- 实现 `detail2::FilePrefetcher`
  - 预读预算按字节数走 `SemaphoreCor`
  - 预读 guard 绑定到 `OpenedFile` 生命周期
  - 仍复用旧的 `OverlappedFileReader` 以保持底层取消和顺序读取行为

  ### 4. pack 侧节点已落地

  已新增：

  - `src/detail2/chunk.hpp`
  - `src/detail2/tar.hpp`
  - `src/detail2/tar.cpp`
  - `src/detail2/zstd.hpp`
  - `src/detail2/zstd.cpp`

  本轮完成内容：

  - 实现 `detail2::TarPacker`
    - 兼容 `OpenedFile` 输入
    - 消费条目时先释放预读预算 guard，再顺序读取文件
    - 输出 tar 数据块时可绑定输出预算 guard
  - 实现 `detail2::ZstdCompressor`
    - 保留原有自适应压缩级别调节逻辑
    - 压缩输出同样可绑定输出预算 guard
  - 实现 `detail2::chunk.hpp`
    - 通过 aliasing `shared_ptr` 让 `DataChunk` 的释放自动回收预算 guard

  ### 5. 顶层编排已切换到 detail2 发送侧

  本轮完成内容：

  - 新增 `src/detail2/orchestration.cpp`
    - `pack_directory_to_file()` 切到 `detail2` 发送侧节点
    - `listen_directory()` 切到 `detail2` 发送侧节点
    - `unpack` / `receive` 先继续复用旧实现，保证行为稳定
  - 新增 `src/detail2/gui_runtime.*`
    - GUI 发送页内部的发送流水线切到 `detail2`
    - GUI 交互状态机与用户体验保持原样

  ### 6. 当前接线状态

  - `pack`、`listen`、GUI 发送页已经改走 `detail2`
  - `unpack`、`receive` 以及解包落盘仍保留旧实现

  ### 7. 文件写入对象已落地

  已新增：

  - `src/detail2/writer.hpp`
  - `src/detail2/writer.cpp`

  本轮完成内容：

  - 实现 `detail2::BufferedFileWriter`
    - 槽位缓冲聚合写入
    - 后台 drain 任务通过 `TaskExecutor` 执行
    - 关闭时等待所有已排队槽位刷入底层 `FileByteSink`
    - 保留取消时终止写入与关闭等待的语义

## 验证

- 已构建通过：`soratransport_core`
- 已构建通过：`soratransport`
- 已构建通过：`fasttar`
- 已构建通过：`soratransport_app`
- 已验证：`fasttar pack -z` / `unpack -z` roundtrip smoke test
- 已验证：`fasttar pack -n` / `unpack -n` roundtrip smoke test

## 下一步

1. 把 `BufferedFileWriter` 接进 `unpack` / `receive` 的落盘链路
2. 把 `unpack` / `receive` 也切到 `detail2`
3. 继续补强符号链接、取消与大文件场景的专项验证