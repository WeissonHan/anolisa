# 存储同步

[English](storage-synchronization.md)

Blaze 定期要求已配置的 `StorageProvider` 同步每个 running sandbox 持有的
数据。manager 不编码 provider 特定的文件或设备操作；
`StorageProvider::flush_dirty` 是唯一的持久化边界。

## Sweep 行为

`storage.flush_interval` 是正 duration，默认值为 30 秒。第一次 sweep 在一个
完整间隔后开始。错过的 tick 会被跳过而不是排队，避免较慢的 provider 形成
无界积压。

每次 sweep 在首次调用 provider 前，先复制状态为 `Running` 的 sandbox
引用。随后逐个取得 sandbox operation lock，并再次检查状态。等待期间已经
改变状态的 sandbox 会被跳过。该过程与 checkpoint、rollback、hibernate、
resume 和 destroy 使用相同的逐 sandbox 串行规则。

provider 失败按 sandbox 隔离。sweep 记录 selected、flushed、skipped 和
failed 数量，然后继续处理其余 sandbox。失败不会改变 lifecycle state，因为
provider 仍持有 slot，后续 sweep 可以再次尝试。

## 关闭顺序

API listener 停止后，daemon 会取消并等待周期任务退出。之后 manager shutdown
才会终止其持有的 backend 并排空 warm 资源。任务退出后，不会有 sweep 与
runtime 清理重叠。

file provider 会同步其独立 slot 制品。其他 provider 可以采用不同持久化
机制，同时保持相同的 manager 顺序与重试行为。
