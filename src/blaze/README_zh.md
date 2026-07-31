# ANOLISA Blaze

[English](README.md)

面向 AI Agent 工作负载的单机 sandbox 编排 daemon。

Blaze 通过 HTTP API 管理 sandbox 实例的完整生命周期，支持策略驱动的后端选择。
它提供 warm pool 预分配、多后端回退（Firecracker → Bubblewrap → Mock）以及
Prometheus 指标导出，设计为 E2B 类编排平台的单机执行代理。

## 特性

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + TCP (`:14159`)
- **策略驱动后端选择** — workload class → 后端优先级列表
- **生命周期状态机** — 9 种状态：Pending、Creating、Running、Paused、
  Checkpointed、RecoveryRequired、Reset、Warm 和 Destroyed
- **Guest 操作** — 对提供 guest endpoint 的运行中后端执行有界命令和文件传输
- **Warm pool 管理** — 预热实例 + 基于 TTL 的 GC
- **模板注册表** — 内存中模板追踪，支持空闲驱逐
- **内核 hook 注册** — 前/后置 hook 状态追踪
- **Prometheus 指标** — 请求计数、实例 gauge、池大小
- **Spawner 后端** — FirecrackerSpawner、BubblewrapSpawner、MockSpawner

## 快速开始

```bash
# 构建
cd src/blaze
cargo build --release

# 运行 daemon（开发环境：覆盖 policy.dir 使用本地示例）
sudo ./target/release/blazed daemon start --config examples/config.toml
# 注意：默认配置设置 policy.dir = /etc/anolisa/blaze/policies。
# 源码开发测试时，创建符号链接或覆盖：
#   sudo mkdir -p /etc/anolisa/blaze
#   sudo ln -s $(pwd)/examples/policies /etc/anolisa/blaze/policies

# 健康检查
curl --unix-socket /run/blaze/api.sock http://localhost/v1/health

# 创建 sandbox
curl -X POST --unix-socket /run/blaze/api.sock http://localhost/v1/sandboxes \
  -H 'Content-Type: application/json' \
  -d '{"workload_class":"agent-tool","image_digest":"sha256:..."}'
```

快速开始使用关闭 Firecracker guest transport 的示例策略，因此没有兼容
guest agent 的镜像不会等待 guest 就绪。只有镜像运行了对应 agent 时才应
启用该 transport。

## 配置

daemon 读取 TOML 配置文件（默认：`/etc/anolisa/blaze/config.toml`）
以及包含按 workload class 划分的策略文件的策略目录。

```
/etc/anolisa/blaze/
├── config.toml
└── policies/
    ├── agent-rl.toml
    └── agent-tool.toml
```

参见 `src/blaze/examples/` 获取带注释的示例配置。

### API 请求上限

daemon 默认接收不超过 1 MiB 的请求体。它会同时检查声明的
`Content-Length` 和逐帧到达的数据；超过配置上限时返回 HTTP 413。
可以通过正整数字节数覆盖默认值：

```toml
[api]
max_body_bytes = 1048576
```

Guest 文件在 base64 解码后最多为 16 MiB。完整的 16 MiB 写入经过 JSON 和
base64 编码后会变大，所以默认 1 MiB 请求上限会拒绝它。调用者确实需要完整
文件上限时，应至少配置 22 MiB：

```toml
[api]
max_body_bytes = 23068672
```

daemon 会同时检查 HTTP 请求大小和解码后的文件大小。

### VM 资源配置

Blaze 使用三层回退链解析 vCPU 和内存设置：

1. **后端特定**（`[backend.firecracker].vcpus` / `.memory`）— 最高优先级
2. **策略级**（`[vm].vcpus` / `[vm].memory`）— 跨后端共享
3. **代码默认值**（1 vCPU, 256 MiB）— 未指定时的兜底

策略文件示例：

```toml
[vm]
vcpus = 2
memory = "512Mi"

[backend.firecracker]
vcpus = 4        # 仅对 Firecracker 覆盖 [vm].vcpus
memory = "1Gi"   # 仅对 Firecracker 覆盖 [vm].memory
```

### 存储配置

`[storage]` 部分控制 sandbox 存储后端：

```toml
[storage]
provider = "file"       # 存储 provider 选择。当前支持："file"、"auto"。
                        # "auto" 按优先级探测可用 provider（当前等同于 "file"）。
                        # 其他值将记录告警并回退到 file。
images_dir = "/var/lib/blaze/images"
# pool_size = 0           # [Reserved] 预热存储槽位数（尚未启用）
# prefork = false         # [Reserved] 是否在槽位中预启动 VM（尚未启用）
# flush_interval = "30s"  # [Reserved] 脏数据刷盘周期（尚未启用）
```

`file` provider 使用标准文件系统操作管理 sandbox 存储。`auto` 按优先级探测可用 provider（当前等同于 `file`）。无法识别的值将记录告警并回退到 `file`。

## API 端点

| 方法 | 路径 | 说明 |
|--------|------|-------------|
| GET | `/v1/health` | 健康检查 |
| GET | `/v1/sandboxes` | 列出所有 sandbox |
| POST | `/v1/sandboxes` | 创建 sandbox |
| GET | `/v1/sandboxes/{id}` | 获取 sandbox 详情 |
| DELETE | `/v1/sandboxes/{id}` | 销毁 sandbox |
| POST | `/v1/sandboxes/{id}/exec` | 执行 guest 命令 |
| POST | `/v1/sandboxes/{id}/read` | 读取 guest 文件 |
| POST | `/v1/sandboxes/{id}/write` | 替换 guest 文件 |
| POST | `/v1/sandboxes/{id}/checkpoint` | 后端和存储 provider 支持时捕获完整 checkpoint |
| GET | `/v1/sandboxes/{id}/checkpoints` | 列出已提交的 checkpoint 及其 HEAD 可达性 |
| POST | `/v1/sandboxes/{id}/rollback/{checkpoint_id}` | 用经过校验的 checkpoint 替换正在运行的 sandbox |
| GET | `/v1/instances` | 列出 sandbox 的兼容入口 |
| POST | `/v1/instances` | 创建 sandbox 的兼容入口 |
| GET | `/v1/instances/{id}` | 获取 sandbox 详情的兼容入口 |
| DELETE | `/v1/instances/{id}` | 销毁 sandbox 的兼容入口 |
| POST | `/v1/instances/{id}/destroy` | 保留的销毁 action |
| POST | `/v1/instances/{id}/exec` | Guest 命令兼容入口 |
| POST | `/v1/instances/{id}/read` | Guest 文件读取兼容入口 |
| POST | `/v1/instances/{id}/write` | Guest 文件写入兼容入口 |
| POST | `/v1/instances/{id}/checkpoint` | 捕获完整 checkpoint 的兼容入口 |
| GET | `/v1/instances/{id}/checkpoints` | 列出 checkpoint 的兼容入口 |
| POST | `/v1/instances/{id}/rollback/{checkpoint_id}` | 恢复 checkpoint 的兼容入口 |
| POST | `/v1/instances/{id}/reset` | 预留接口；运行时重置实现前返回 `501` |
| GET | `/v1/pools` | 列出 warm pool |
| GET | `/v1/pools/{backend}/{class}` | 获取 pool 状态 |
| POST | `/v1/pools/{backend}/{class}/drain` | 排空 pool |
| PUT | `/v1/pools/{backend}/{class}/sizing` | 调整 pool 大小 |
| GET | `/v1/templates` | 列出模板 |
| GET | `/v1/templates/{id}` | 查看模板详情 |
| POST | `/v1/templates/gc` | 触发模板 GC |
| GET | `/v1/policies` | 列出已加载策略 |
| GET | `/v1/hooks` | 列出内核 hook |
| GET | `/v1/metrics` | Prometheus 指标 |
| POST | `/v1/admin/reload` | 热加载策略 |

### 生命周期管理与恢复

创建和销毁会在修改存储或后端资源之前记录当前操作。创建成功后状态为
`Running`，销毁成功后状态为 `Destroyed`。如果失败补偿不能释放全部已有
资源，sandbox 会保留为可查询的 `RecoveryRequired`，后续可以再次执行销毁。

daemon 启动时会逐个处理未结束的 sandbox。单个 sandbox 清理失败不会阻止
其他记录继续处理，也不会阻止 API 启动。

正常关闭时，daemon 会先停止接收新请求并等待已有连接结束，再为每条持久化
记录和仍持有的后端资源执行有界清理。单条清理失败不会跳过其余 sandbox，
所有未完成记录都会汇总报告。

创建和销毁的操作记录会保存操作类型及开始时间。checkpoint 还会记录生成的
checkpoint ID，以及 daemon 已确认的最近一次持久化边界。checkpoint 列表会
另外报告中断后实际可见的 catalog 记录和 HEAD 更新。中断的创建会被清理而
不是从原位置继续，重启后也不会接管先前的后端进程。恢复失败后目前没有后台
循环自动重试。

只有所选后端和当前存储 provider 都声明支持完整捕获时，checkpoint 才可用。
否则 daemon 会在创建操作记录、暂停后端或修改 checkpoint catalog 之前返回
`501`。一次成功的捕获会：

1. 暂停后端，并捕获完整的 VM 状态和内存；
2. 刷新当前存储 slot，并复制完整根文件系统；
3. 发布经过校验的 checkpoint，并推进 HEAD；
4. 恢复后端，确认 guest 已经就绪后再返回 `Running`。

file storage provider 会把完整根文件系统复制到每个 checkpoint。与共享 base
的格式相比，这会占用更多空间，但每个 checkpoint 都不依赖 live slot 后续的
变化。

如果在调用 catalog 发布步骤之前发现失败，daemon 会恢复后端并删除未完成的
stage。如果发布或 HEAD 的结果无法确定，或者后端无法恢复，sandbox 会进入
`RecoveryRequired`；runtime ownership 和已经提交的 checkpoint 数据仍会
保留，供后续显式清理。列出 checkpoint 与捕获、guest 操作及销毁共用同一个
sandbox 操作锁。销毁会删除事务临时文件，但保留已经提交的 checkpoint 历史。

只有当前存储 provider 和 checkpoint 对应的后端都实现恢复，并且当前后端版本
与捕获时记录的版本完全一致，daemon 才会开始恢复。修改 runtime 之前，daemon
会先校验所选 checkpoint、完整父链和全部 artifact hash。

file provider 会在旧后端仍然运行时准备一份独立的 rootfs。旧后端停止后，
daemon 才选择这份 rootfs，启动并持有新的后端，随后把 HEAD 指向所选
checkpoint，最后释放旧 rootfs。旧后端停止前发生失败时，原 runtime 会继续
运行；停止后发生任何无法确认的失败时，daemon 会保留实际存在的资源，并把
sandbox 标记为 `RecoveryRequired`，后续 destroy 仍能找到并清理这些资源。

`last_checkpoint` 始终表示最近一次成功捕获。恢复只移动 catalog HEAD，不会
改写捕获历史。

runtime reset 仍是预留接口，会返回 `501`，且不会修改 runtime 或持久化状态。

### Guest 操作

只有 sandbox 处于 `Running` 且后端报告了 guest endpoint 时，才能执行
guest 操作。冷启动后端如果报告了该 endpoint，创建流程会等待 guest agent
响应后才发布 `Running`。关闭 guest 支持的后端会跳过等待，后续 guest
操作返回 HTTP 409。当前从 warm pool 激活实例时不会再次执行 guest
readiness 探测。

Guest 操作和生命周期变更使用同一个 sandbox 操作锁。请求可能等待先开始的
生命周期操作；取得锁后，manager 会再次检查 `Running`。如果 destroy 或
其他状态变更先完成，guest 请求不会访问旧 runtime，而是直接失败。

接口接收以下 JSON：

```json
{"cmd":"uname -a","cwd":"/","env":{"LANG":"C"},"timeout":10}
```

```json
{"path":"/tmp/input","data_b64":"aGVsbG8="}
```

`read` 只需要 `path`；文件读取结果和命令输出使用标准 base64。Exec timeout
范围为 1 至 20 秒。Guest 文件解码后最多为 16 MiB，响应帧也有固定上限。

如果 exec 或 write 在请求送达前失败，它是普通通信失败；送达前等待超时使用
`"code": "guest_timeout"`。如果已经开始送达，但 daemon 无法确定结果，API
返回 HTTP 504 和 `"code": "guest_outcome_unknown"`；调用者应先核对状态，
不能自动重放。read 不改变 guest 状态，可以由调用者决定是否重试。Guest
read 返回内容过大时，API 返回 HTTP 502 和
`"code": "guest_response_too_large"`。exec 或 write 已经开始送达后，如果
返回内容过大或不可信，结果仍归为 unknown。调用者的请求过大时返回 HTTP
413。

每个请求都会完整缓冲。上限约束的是单个请求，而不是所有并发请求之和，因此
调用方还需要限制 guest 操作并发数。当前不支持文件流式传输、交互式终端和
会话复用。

#### 健康检查

`GET /v1/health` 返回 daemon 状态，包含存储池就绪信息：

```json
{
  "status": "ok",
  "version": "0.3.0",
  "storage_pool": { "ready": 0, "capacity": 0, "pending": 0 }
}
```

## 项目结构

```
src/blaze/
├── crates/
│   ├── blaze-core/   # 库：策略、生命周期、池、模板、内核、配置
│   └── blazed/       # 二进制：daemon、API server、spawner、指标
├── examples/         # config.toml、policies/
├── dist/             # blazed.service、blaze.spec、tmpfiles
└── manifests/        # 组件元数据
```

## 环境要求

- Rust 1.88+（参见 `src/blaze/rust-toolchain.toml`）
- 具有 root 权限的 Linux 主机（sandbox 后端需要）

## 许可证

Apache-2.0
