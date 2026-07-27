# 受管 Sandbox 生命周期

[English](managed-lifecycle.md)

Blaze 使用一个 `SandboxManager` 统一持有持久化元数据、provider slot、backend
handle 和 warm-pool claim。后端特定工作保留在 `BackendSpawner` 与
`BackendInstance` 后面；provider 特定的分配与清理保留在 `StorageProvider`
后面。因此，生命周期补偿逻辑不依赖具体 runtime 或存储实现。

manager 是内部服务。HTTP 路由和面向客户端的请求 schema 属于独立职责，不由
本组件定义。

## 持久化操作边界

每个多步骤变更都会先记录 operation，再修改 runtime 资源。状态先写入临时
文件并同步，随后原子重命名并同步父目录。因此，状态文件写入成功就是控制面
状态转换的 commit point。

create 按以下顺序执行：

1. 校验可选的指定 UUID 和已存在的持久化记录。
2. 持久化带有 `Create` operation marker 的 `Creating` 状态。
3. 取得兼容的 warm slot，或从 provider 新分配 slot。
4. 启动或复用 backend；启用 guest 时等待其就绪。
5. 保存 live runtime handle，持久化 `Running`，并清除 marker。

持久化 `Destroyed` 后，destroy 是幂等操作。在此之前，manager 会串行访问
runtime，依次停止 backend、删除 runtime 制品、释放 provider slot，最后才
提交终态。

后续步骤失败时，清理按照资源持有顺序逆向执行。若清理本身失败，manager
不会丢弃剩余 handle 或 slot，而是继续持有它并提交 `RecoveryRequired`，使
后续重试能明确识别仍需清理的资源。

## Runtime 持有与并发

可序列化的 `SandboxInstance` 与不可序列化的 runtime handle 分别保存在不同
map 中。全局 map lock 只用于短暂查找和更新，绝不跨越 `.await`。每个 live
runtime 有独立的异步 mutex，因此同一 sandbox 的操作保持有序，同时不阻塞
其他 sandbox。

supervisor 等待每个 backend instance。仅当观察到的 handle 仍是当前 Running
handle 时，意外退出才会更新状态，并将 sandbox 标为 `RecoveryRequired`。
已经被替换或销毁的 runtime 不受影响。

启动时，若某条记录无法证明已经终结或能够独立重建，由于进程内 handle 已经
丢失，manager 会将其标为待恢复。reconciliation 让 spawner 清理标识匹配的
orphan，并让 provider 根据稳定 sandbox ID 重新构造路径。

## 就绪契约

guest 就绪检查在 backend socket proxy 上使用小型 JSON-line 协议。每次尝试
都会建立新连接，发送 `CONNECT 5000`，要求 `OK` 响应包含数字形式的 peer
identifier，然后发送带关联 ID 的 `ping` 请求。响应行有长度上限，并且必须
返回同一请求 ID。

轮询同时具备总 deadline、较短的单次 deadline、指数退避和 shutdown
cancellation。停滞或格式错误的连接不会影响下一次尝试。命令执行和 guest
文件操作不属于本契约。

## Warm-pool 规则

异步 runtime pool 可以只准备存储 slot，也可以预启动一个尚未分配的 backend。
预启动 slot 只有通过就绪检查后才会进入 ready 队列。

第一个符合条件的请求会固定一份不可变 runtime prototype，包括可执行文件、
backend 设置、VM 设置和网络设置。prototype 不同的请求走普通分配路径，不会
取得不兼容 slot。并发 refill 不会超过配置的 target；drain 或 shutdown 会
等待 pending builder 结束，再释放 ready 资源。

builder 失败时会尽可能释放资源。若 release 也失败，slot 会进入 quarantine，
而不会被报告为 ready。这里选择显式减少容量，而不是复用持有关系不确定的资源。

## 当前边界

本设计中的 manager 负责 create、list、inspect、destroy、pool status、pool
cleanup、启动 reconciliation 和 shutdown cleanup。持久化状态词汇还预留了
checkpoint 与 hibernation 服务使用的事务状态；其操作顺序详见
[恢复事务](recovery-transactions_zh.md)。

每个 manager 同时只使用一个 warm-pool prototype。file provider 为各 slot
使用自包含文件，因此 warm allocation 会增加容量占用和复制时间，以换取独立
清理与 restore 的正确性。
