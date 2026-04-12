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

## 验证

- 已构建通过：`soratransport_core`

## 下一步

1. 在 `detail2` 中重写文件遍历器、文件打开器和文件预读器
2. 用 `SemaphoreCor` 替换这些节点上的预算与并发控制
3. 再切换顶层编排到新节点，并继续保持旧行为不变