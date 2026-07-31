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
- **Runtime 槽位容量** — 独立存储槽位、可选后端 prefork 和基于 TTL 的清理
- **模板注册表** — 内存中模板追踪，支持空闲驱逐
- **内核 hook 注册** — 前/后置 hook 状态追踪
- **Prometheus 指标** — 请求计数、实例 gauge、池大小
- **Spawner 后端** — FirecrackerSpawner、BubblewrapSpawner、MockSpawner
- **可选 VM 网络** — 每台 Firecracker VM 独立使用 netns、tap、veth 和 NAT

## 安装

Blaze 当前是 Labs 组件。源码树中包含 ANOLISA 组件清单和 RPM 打包文件，但
配置的组件仓库不一定发布 `blaze` 候选包。执行系统级安装前，先预览仓库
解析结果：

```bash
sudo anolisa --install-mode system --dry-run install blaze
sudo anolisa --install-mode system install blaze
```

如果 RPM 仓库发布了 Blaze：

```bash
sudo yum install blaze
```

开发者从源码构建：

```bash
cd src/blaze
cargo build --release --locked
```

## 快速开始

```bash
# 选择一种启动方式，不要同时运行两种方式。
# 软件包安装
sudo systemctl enable --now blazed

# 源码构建方式（覆盖 policy.dir 使用本地示例）
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
enable_network = false
```

设置 `enable_network = true` 后，每台 Firecracker VM 会获得独立的网络
slot。显式销毁 sandbox 和启动失败补偿会在进程确认终止后删除对应的 netns、
tap 和 veth。daemon 重启后再次销毁时可以根据记录恢复清理，但不会在后台
自动扫描。slot 创建和删除使用主机级锁，避免多个 daemon 同时分配相同的主机
设备名。加载的 Firecracker 策略启用该选项时，backend probe 还会检查所需
命令和主机权限；网络关闭时跳过这些检查。上游路由和 DNS 仍由主机运维方
配置。

### 存储配置

`[storage]` 部分控制 sandbox 存储后端：

```toml
[storage]
provider = "file"       # 存储 provider 选择。当前支持："file"、"auto"。
                        # "auto" 按优先级探测可用 provider（当前等同于 "file"）。
                        # 其他值将记录告警并回退到 file。
images_dir = "/var/lib/blaze/images"
pool_size = 0            # 后台运行槽位数；0 表示不构建
prefork = false          # 槽位就绪前是否启动后端
# flush_interval = "30s"  # [Reserved] 脏数据刷盘周期（尚未启用）

[pool]
default_warm_ttl = "30m" # 符合条件的策略未设置 warm_ttl 时使用
gc_interval = "5m"       # 过期检查和容量维护间隔
```

`file` provider 使用标准文件系统操作管理 sandbox 存储。`auto` 按优先级探测可用 provider（当前等同于 `file`）。无法识别的值将记录告警并回退到 `file`。

`pool_size` 非零时，首个符合条件的创建请求会固定一组兼容构建参数，并启动
后台补充。每个槽位都持有存储；只有启用 `prefork` 时，槽位才同时持有已经
运行的后端。`pool_size` 限制池内及正在构建或交接的槽位，不限制已经完成
生命周期交接的 sandbox。参数不兼容的请求会继续走已有创建流程。
只有策略的 `[pool]` 设置 `enabled = true` 时，该策略才符合条件；策略中
可选的 `warm_ttl` 会覆盖 `default_warm_ttl`。

daemon 重启时会在接收请求前清理尚未交接的槽位记录，不会把旧槽位重新放回
ready 队列。该清理使用重启后当前配置的 provider 以及存储和运行目录，因此
这些设置必须继续指向同一组已有目录。下文的 `/v1/pools` 描述的是另一套
生命周期回收 pool 的管理接口，不展示这里的后台运行容量。公开 reset 当前
返回 `501`，因此没有生产路径把已经使用的 sandbox 放回该 pool。

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
| GET | `/v1/instances` | 列出 sandbox 的兼容入口 |
| POST | `/v1/instances` | 创建 sandbox 的兼容入口 |
| GET | `/v1/instances/{id}` | 获取 sandbox 详情的兼容入口 |
| DELETE | `/v1/instances/{id}` | 销毁 sandbox 的兼容入口 |
| POST | `/v1/instances/{id}/destroy` | 保留的销毁 action |
| POST | `/v1/instances/{id}/exec` | Guest 命令兼容入口 |
| POST | `/v1/instances/{id}/read` | Guest 文件读取兼容入口 |
| POST | `/v1/instances/{id}/write` | Guest 文件写入兼容入口 |
| POST | `/v1/instances/{id}/checkpoint` | 预留接口；后端和存储快照实现前返回 `501` |
| POST | `/v1/instances/{id}/reset` | 预留接口；运行时重置实现前返回 `501` |
| GET | `/v1/pools` | 列出生命周期回收 pool |
| GET | `/v1/pools/{backend}/{class}` | 获取生命周期回收 pool 状态 |
| POST | `/v1/pools/{backend}/{class}/drain` | 排空生命周期回收 pool |
| PUT | `/v1/pools/{backend}/{class}/sizing` | 调整生命周期回收 pool 大小 |
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

runtime 槽位核对会在该生命周期处理之前完成；inventory、journal 或 runtime
清理失败会停止启动。runtime 核对成功后，daemon 会逐个处理未结束的
sandbox。单个 sandbox 清理失败不会阻止其他记录继续处理，也不会阻止 API
启动。

正常关闭时，daemon 会先停止接收新请求并等待已有连接结束，再为每条持久化
记录和仍持有的后端资源执行有界清理。单条清理失败不会跳过其余 sandbox，
所有未完成记录都会汇总报告。

操作记录只保存操作类型和开始时间，不记录每个资源步骤是否已经完成。中断的
创建会被清理而不是从原位置继续，重启后也不会接管先前的后端进程。恢复失败
后目前没有后台循环自动重试。checkpoint 和 reset 会先确认 sandbox 处于
`Running` 且没有进行中的生命周期操作，然后返回 `501`；它们不会修改运行
资源或持久化状态，其后端和存储操作尚未在这里实现。

### Guest 操作

只有 sandbox 处于 `Running` 且后端报告了 guest endpoint 时，才能执行
guest 操作。冷启动后端如果报告了该 endpoint，创建流程会等待 guest agent
响应后才发布 `Running`。关闭 guest 支持的后端会跳过等待，后续 guest
操作返回 HTTP 409。后端提供 guest endpoint 时，prefork 槽位会在进入
ready 前等待 guest readiness；取用时再次检查后端存活，但不重复 guest
readiness。仅含存储的槽位会在启动后端后等待 guest readiness。

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

`GET /v1/health` 返回 daemon 状态，包含 provider 存储池就绪信息：

```json
{
  "status": "ok",
  "version": "0.3.0",
  "storage_pool": { "ready": 0, "capacity": 0, "pending": 0 }
}
```

`storage_pool` 对象不报告后台 runtime 槽位容量。

## 文档

- [Runtime 槽位用户指南](../../docs/user-guide/zh/runtime/blaze/QUICKSTART.md)
- [Runtime 槽位 ownership 设计](docs/design/runtime-slot-ownership.md)

## 项目结构

```
src/blaze/
├── crates/
│   ├── blaze-core/   # 库：策略、生命周期、池、模板、内核、配置
│   └── blazed/       # 二进制：daemon、API server、spawner、指标
├── docs/design/       # 组件设计文档
├── examples/         # config.toml、policies/
├── dist/             # blazed.service、blaze.spec、tmpfiles
└── manifests/        # 组件元数据
```

## 环境要求

- Rust 1.88+（参见 `src/blaze/rust-toolchain.toml`）
- 具有 root 权限的 Linux 主机（sandbox 后端需要）
- 启用 VM 网络时需要 `ip`、`iptables`、`sysctl` 和 netns 管理权限

## 许可证

Apache-2.0
