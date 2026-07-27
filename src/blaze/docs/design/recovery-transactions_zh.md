# 恢复事务

[English](recovery-transactions.md)

Blaze 将 checkpoint、rollback、hibernate 和 resume 作为由
`SandboxManager` 持有的持久化事务。后端特定的 pause、snapshot、restore
与终止行为保留在 `BackendInstance` 和 `BackendSpawner` 后面；provider 特定
的数据同步保留在 `StorageProvider` 后面。

每个操作都会先在 sandbox 状态中记录 intent，再修改 runtime 资源。只有最终
状态和 operation marker 清除都已持久化，操作才可以报告成功。若补偿无法恢复
旧状态，Blaze 会保留仍可识别的全部资源，并提交 `RecoveryRequired`。

## Checkpoint store

`CheckpointStore` 为每个 sandbox 保存一个目录，并为每个已提交 checkpoint
保存一个不可变目录。checkpoint 包含：

- `vmstate.snap`
- `mem.diff`
- `rootfs.diff`
- `metadata.json`

metadata 记录 sandbox identity、template 与 image identity、backend 与其
版本、父 checkpoint、snapshot kind，以及每个必要制品的逻辑大小和 SHA-256
摘要。

checkpoint 从唯一的隐藏 staging directory 开始创建。commit 会同步各制品和
metadata 文件、同步 staging directory、将其原子重命名为最终 checkpoint
ID，并同步 sandbox checkpoint directory。另一个原子 HEAD 文件标识当前选择
的 lineage。

发布 checkpoint 与移动 HEAD 是两个独立 commit point。两者之间发生中断时，
会留下不可变但不可达的 checkpoint。它不会被 restore 选中，可以由运维检查，
也可以被 prune 删除。

校验会拒绝格式错误的 ID、路径穿越、缺失制品、identity 变化、大小或摘要变化、
需要但缺失的 backend 版本、父链循环、跨 sandbox parent，以及无法解析到有效
root 的 chain。

## Checkpoint 事务

checkpoint 事务按以下顺序执行：

1. 持久化 `Checkpoint` marker。
2. 暂停当前 backend，并持久化 `Paused`。
3. 将 backend 状态写入 checkpoint staging directory。
4. flush provider 数据，并复制 root filesystem 制品。
5. 发布已校验 checkpoint 并移动 HEAD。
6. 恢复原 backend；启用 guest 时重新检查就绪。
7. 持久化 `Running`、所选 checkpoint ID，并清除 marker。

HEAD 移动前发生失败时，事务会删除 staging 并尝试恢复原 backend。发布后失败
可能留下不可达 checkpoint。HEAD 移动后发生失败时，恢复逻辑会保留 HEAD 和
operation marker 供诊断，而不会声称该事务从未发生。

## Rollback 事务

rollback 在修改当前 runtime 之前，会校验目标 checkpoint 及其 identity。
随后持久化 `RollingBack`、停止当前 backend，并在 provider slot 中保留当前
root filesystem 的备份。只有安装 checkpoint root filesystem 后，才 restore
backend snapshot。

恢复后的 backend 必须通过就绪检查，才能提交 HEAD 和 sandbox 状态。旧 root
filesystem 备份一直保留到最终 commit。若 replacement backend 失败后的清理
也失败，新 handle 会被保留，sandbox 进入 `RecoveryRequired`。

每次 checkpoint 创建都会生成新 ID。允许重复 rollback 到同一个有效
checkpoint，所选 lineage 保持一致。

## Hibernate 与 resume

hibernate 保留 provider slot，但释放 live backend：

1. 持久化 `Hibernating`。
2. pause，并 snapshot 到唯一 staging directory。
3. flush provider 数据并同步 snapshot 制品。
4. 终止 backend。
5. 原子发布 hibernate directory。
6. 持久化 `Hibernated` 并清除 operation marker。

resume 在需要时重新构造 provider slot，持久化 `Resuming`，从已发布
hibernate 制品 restore backend，等待其就绪，最后提交 `Running`。

hibernate 失败时会尽可能恢复原 backend。resume 失败时会终止 replacement
backend，并尽可能恢复 `Hibernated`。这些补偿若失败，状态进入
`RecoveryRequired`。destroy 会先删除已发布或 staging 中的 hibernation
制品，再释放 provider slot。

## File provider 取舍

file provider 为每个 checkpoint 记录完整 root filesystem 制品，并让
hibernated 存储继续保留在其独立 slot 中。这比共享 copy-on-write layer 使用
更多容量、耗时更长，但 checkpoint 不依赖其他 sandbox 的可变文件。删除或修改
其他 slot 不会破坏 restore。

## 并发与当前边界

checkpoint、rollback、hibernate、resume、prune 和 destroy 都在 sandbox
runtime mutex 上串行。文件 hash 和 lineage 遍历在 blocking worker 中运行，
避免阻塞异步 executor。

这些服务操作 manager method 和 backend/provider trait。HTTP 路由、guest
命令执行与 guest 文件传输不属于它们的职责。
