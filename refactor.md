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

## 验证

- 已构建通过：`soratransport_core`

## 下一步

1. 重写 detail2 打包器与压缩节点，并把输出预算迁移到信号量 guard
2. 切换 pack/listen 的顶层编排到新节点
3. 继续补齐 unpack/写入路径并保持旧行为不变