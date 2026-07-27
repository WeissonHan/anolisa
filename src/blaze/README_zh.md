# ANOLISA Blaze

[English](README.md)

面向 AI Agent 工作负载的单机 sandbox 编排 daemon。

Blaze 通过 daemon-only HTTP API 管理 sandbox 生命周期，并由策略选择后端。
它通过与后端无关的接口持有后端进程、guest 操作、warm runtime 容量、
checkpoint 与 hibernate 事务，以及模板导入。

## 特性

- **HTTP API** — Unix domain socket (`/run/blaze/api.sock`) + 可选 TCP
- **策略驱动后端选择** — workload class → 后端优先级列表
- **生命周期事务** — 持久化 operation marker 与可恢复的资源持有关系
- **运行时 warm pool** — 存储预分配或预启动 backend，并异步补充容量
- **Guest 操作** — 有界的就绪检查、命令执行与文件传输
- **Checkpoint 与 hibernate** — 制品校验、rollback、prune、hibernate 与 resume
- **模板 API** — 事务式导入自包含模板制品
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
instances_dir = "/var/lib/blaze/instances"
pool_size = 0            # ready slot 目标值
prefork = false          # 为每个 ready slot 预启动 backend
flush_interval = "30s"   # 同步 running provider slot
rootfs_size = 8589934592
mem_size = 4294967296

[api]
max_body_bytes = 1048576
max_file_bytes = 16777216
request_timeout = "30s"
```

`file` provider 为每个实例提供独立的 root filesystem 和 memory 文件。
`images_dir` 与 `instances_dir` 必须互不重叠；相同或互为父子目录的路径会被
拒绝。独立副本会使用更多容量并增加 create 延迟，但不依赖其他实例文件也能
保持有效。`StorageProvider` interface 允许其他实现优化这一取舍。`auto`
当前等同于 `file`；无法识别的值会记录告警并回退到它。
运行时池使用第一个符合条件的 policy 初始化；runtime prototype 不同的请求
使用普通分配路径。
daemon 按配置间隔同步每个 running slot。单个 provider 失败会被报告，但不会
终止剩余 sweep；关闭时会在清理 runtime 之前等待该循环退出。

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
| GET | `/v1/pool/status` | 返回当前 ready、capacity 与 pending |
| POST | `/v1/pool/cleanup` | 排空 ready 资源并触发 refill |
| GET | `/v1/templates` | 列出模板 |
| GET | `/v1/templates/{id}` | 查看模板详情 |
| POST | `/v1/templates/import` | 事务式导入模板目录 |
| POST | `/v1/templates/gc` | 触发模板 GC |
| GET | `/v1/policies` | 列出已加载策略 |
| GET | `/v1/hooks` | 列出内核 hook |
| GET | `/v1/metrics` | Prometheus 指标 |
| POST | `/v1/admin/reload` | 热加载策略 |

`/v1/instances` 及现有 instance 操作继续作为兼容别名，并调用同一个
sandbox manager。API 错误包含 `code`、`message`、`operation` 和
`sandbox_id`。

create 可接收可选 UUID。成功后以同一 UUID 和相同不可变参数重复创建会返回
现有 running sandbox，参数不一致则返回 `409`。destroy 幂等。checkpoint
每次成功都会提交新 ID；可以重复 rollback 到同一个 checkpoint。hibernate
或 resume 成功后再次调用会因源状态前置条件不满足而返回 state conflict。

请求契约、限制、模板布局和重试行为详见
[Sandbox 管理 API](docs/design/management-api_zh.md)。
[存储同步](docs/design/storage-synchronization_zh.md)说明周期性 provider
契约与关闭顺序。

#### 健康检查

`GET /v1/health` 返回 daemon 状态，包含存储池就绪信息：

```json
{
  "status": "ok",
  "version": "0.4.0",
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
- 具有 root 权限的 Linux 主机（sandbox 后端需要）

## 许可证

Apache-2.0
