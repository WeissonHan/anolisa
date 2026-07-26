# ANOLISA Blaze

[English](README.md)

面向 AI Agent 工作负载的单机 sandbox 编排 daemon。

Blaze 通过 daemon-only HTTP API 管理 sandbox 完整生命周期，并由策略选择后端。
它提供 Firecracker 进程所有权、Guest Agent I/O、异步运行时 warm pool、
可恢复的 hibernate/resume 与 checkpoint/rollback 事务，以及后台存储同步。

## 特性

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + 可选 TCP
  （TCP 不内置认证或 TLS，只能暴露在可信管理网络）
- **策略驱动后端选择** — workload class → 后端优先级列表
- **生命周期事务** — 13 种状态、持久化 operation journal 和 `RecoveryRequired`
- **运行时 warm pool** — 存储预分配或运行中待分配 VM pre-fork，异步 refill 和真实资源 drain
- **Firecracker 所有权** — API 就绪、进程监督、pause/resume、snapshot/restore、可选 netns/tap/NAT
- **Guest Agent** — 有界 Firecracker CONNECT + JSON-line ping/exec/read/write
- **Checkpoint 与 hibernate** — SHA-256 artifact manifest、HEAD 链、rollback、prune、hibernate/resume
- **模板 API** — 事务式导入 sandbox 模板制品
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
  -d '{"workload_class":"agent-rl","image_digest":"sha256:..."}'
```

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
instances_dir = "/var/lib/blaze/instances" # 必须与 images_dir 互不包含
pool_size = 0            # ready slot 目标值
prefork = false          # true 时每个 slot 还会启动一个 ready、待分配 VM
flush_interval = "30s"   # 定期 fsync provider 持有的 slot 制品
rootfs_size = 8589934592 # 无基础文件时的稀疏文件大小（字节）
mem_size = 4294967296

[api]
max_body_bytes = 1048576
max_file_bytes = 16777216
request_timeout = "30s"
```

存在 `images_dir/rootfs.ext4` 和 `images_dir/mem.bin` 时，file provider
会复制它们；否则创建稀疏文件。每个 slot 都持有独立文件，会使用更多容量并增加
create 延迟，但 restore 不依赖其他 slot。相同或互为父子目录的存储 root 会被
拒绝。`StorageProvider` interface 允许其他实现优化这一取舍。运行时池使用
第一个符合条件的 policy 初始化；runtime prototype 不同的请求走 cold path。

Firecracker policy 中的 `enable_vsock` 与 `enable_network` 分别启用 Guest Agent
和隔离网络数据面。

### 后端主机要求

Firecracker 需要 Linux、root 权限以及 `ip`、`unshare` 可执行文件。policy
启用 VM 网络后，Blaze 会创建独立 namespace 和 link pair。主机路由、转发
策略、DNS 和上游连通性由运维人员管理。

进程持有、存储、网络与恢复行为详见
[Runtime 基础设计](docs/design/runtime-foundations_zh.md)。

## API 端点

| 方法 | 路径 | 说明 |
|--------|------|-------------|
| GET | `/v1/health` | 健康检查 |
| GET, POST | `/v1/sandboxes` | 列出或创建 sandbox |
| GET, DELETE | `/v1/sandboxes/{id}` | 查看或幂等销毁 sandbox |
| POST | `/v1/sandboxes/{id}/exec` | 执行 guest 命令 |
| POST | `/v1/sandboxes/{id}/read` | 以标准 base64 读取 guest 文件 |
| POST | `/v1/sandboxes/{id}/write` | 写入标准 base64 guest 文件 |
| POST | `/v1/sandboxes/{id}/checkpoint` | 提交 checkpoint |
| GET | `/v1/sandboxes/{id}/checkpoints` | 列出 checkpoint 与 HEAD 可达性 |
| POST | `/v1/sandboxes/{id}/checkpoints/prune` | 删除 HEAD 不可达分支 |
| POST | `/v1/sandboxes/{id}/rollback/{checkpoint}` | 校验并恢复 checkpoint |
| POST | `/v1/sandboxes/{id}/hibernate` | 快照并停止 backend |
| POST | `/v1/sandboxes/{id}/resume` | 恢复 hibernated backend |
| GET | `/v1/pool/status` | 返回真实 ready/capacity/pending |
| POST | `/v1/pool/cleanup` | 排空真实资源并触发 refill |
| GET | `/v1/templates` | 列出模板 |
| GET | `/v1/templates/{id}` | 查看模板详情 |
| POST | `/v1/templates/import` | 事务式导入模板目录 |
| POST | `/v1/templates/gc` | 触发模板 GC |
| GET | `/v1/policies` | 列出已加载策略 |
| GET | `/v1/hooks` | 列出内核 hook |
| GET | `/v1/metrics` | Prometheus 指标 |
| POST | `/v1/admin/reload` | 热加载策略 |

`/v1/instances` 及旧版 instance 操作继续作为兼容别名，并调用同一个
sandbox manager。API 错误固定包含 `code`、`message`、`operation` 和
`sandbox_id`。客户端指定的 create ID 必须是 UUID；成功后以同一 UUID
和相同不可变参数重复创建会返回现有 Running sandbox，参数不一致则返回
`409`。destroy 幂等。checkpoint 创建有意保持非幂等，每次成功都会提交新的
checkpoint ID；可以重复 rollback 到同一个 checkpoint，且结果状态相同。
hibernate 或 resume 成功后再次调用会因源状态前置条件不满足而返回 state
conflict。API 不解析 idempotency-key header。

#### 健康检查

`GET /v1/health` 返回 daemon 状态，包含存储池就绪信息：

```json
{
  "status": "ok",
  "version": "0.3.0",
  "backend": "mock",
  "storage_pool": { "ready": 0, "capacity": 0, "pending": 0 }
}
```

## 项目结构

```
src/blaze/
├── crates/
│   ├── blaze-core/   # 契约：策略、生命周期、checkpoint、guest、storage
│   └── blazed/       # daemon：API、manager、pool、guest client、spawner
├── examples/         # config.toml、policies/
├── dist/             # blazed.service、blaze.spec、tmpfiles
└── manifests/        # 组件元数据
```

## 环境要求

- Rust 1.88+（参见 `src/blaze/rust-toolchain.toml`）
- 仅支持 Linux；不要在 macOS/Windows 构建或测试 Blaze
- Firecracker 验收需要 root、KVM、mount namespace、netns、tap 与 iptables 能力

## 许可证

Apache-2.0
