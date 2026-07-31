# 存储同步

[English](storage-synchronization.md)

Blaze 可以定期要求已配置的 `StorageProvider` 同步 running sandbox 持有的
数据。这样，provider 不仅具备同步单个 slot 的能力，daemon 也能安全调度
所有符合条件的 sandbox。

周期同步默认关闭。将 `storage.flush_interval` 设置为正数 duration 后启用。
`storage.flush_timeout` 是单次 provider 调用的最长时间，默认值为 30 秒。

## 哪些 sandbox 会被同步

每轮开始时，manager 先选择 lifecycle 状态为 `Running` 的记录。调用某个
sandbox 的 provider 前，会取得 create 和 destroy 共用的 operation lock，
然后重新检查记录。

只有同时满足以下条件时才会调用 provider：

- lifecycle 状态仍为 `Running`；
- 没有未结束的 lifecycle operation；
- metadata 记录 backend 正在运行，而且 daemon 仍持有该 backend；
- provider 可以根据 sandbox ID 重建完整 slot。

等待 operation lock 期间改变状态的 sandbox 会被跳过。状态为 Running 但
ownership 不完整的记录会计为失败，而不是被静默遗漏；本轮的其他 sandbox
仍会继续处理。

第一次 sweep 在一个完整 interval 后开始。错过的 tick 会被跳过，不会排队，
因此耗时较长的一轮不会形成无界积压。

## 失败与重试

每次 provider 调用都有独立 deadline。失败或超时后，slot 仍归 sandbox
持有，lifecycle 状态也不会改变；后续 sweep 或 destroy 可以再次尝试。

`StorageProvider::flush_dirty` 是 provider 特定的持久化边界。实现必须保证
调用被取消后仍可安全重试或释放。file provider 会同步独立 sandbox slot
中的规范文件；其他 provider 可以采用不同机制，但必须保持相同的 ownership
和取消合同。

存储同步不会保存 VM 内存或设备状态，不能代替完整 runtime 的保存和恢复。

## Daemon 关闭

daemon 提供请求服务时会同时监控周期 worker。如果 worker 意外退出，daemon
会停止接收工作，并进入正常的协调关闭流程。

正常关闭按以下顺序进行：

1. 停止接收新连接；
2. 取消并等待同步 worker 退出；
3. 排空已经接收的连接；
4. 释放仍持有的 runtime 和 storage 资源。

destroy 开始前 worker 已经退出，因此周期 provider 调用不会和同一
sandbox 的清理并发。如果 worker 与后续关闭阶段都失败，daemon 会同时报告
两项失败。
