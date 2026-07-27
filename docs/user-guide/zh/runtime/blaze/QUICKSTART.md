# Blaze CLI 用户指南

[English](../../../en/runtime/blaze/QUICKSTART.md)

Blaze 为管理员提供统一的本地控制面，用于 sandbox 生命周期、Guest I/O、
checkpoint 和 warm-pool 维护。`blazed` binary 持有全部编排状态和数据面资源；
`blazectl` 是独立 HTTP client，只执行本文档列出的远程操作。

## 安装

Blaze 是 Linux system component。默认 Unix socket 不对所有用户开放，因此
初始访问边界是 service administrator/root。

### ANOLISA component manager

当已配置的 component catalog 包含 Blaze 时，使用 system mode：

```bash
sudo anolisa --install-mode system install blaze
sudo systemctl enable --now blazed.service
```

Catalog 是否可用取决于已配置的 Linux 发行版仓库；源码树本身不会发布 package。

### RPM package

在仓库包含 Blaze 的受支持 RPM-based 发行版上：

```bash
sudo yum install blaze
sudo systemctl enable --now blazed.service
```

Package 契约如下：

| Artifact | Owner/mode | 用途 |
|----------|------------|------|
| `/usr/bin/blazectl` | `root:root`, `0755` | 管理员 HTTP client |
| `/usr/libexec/anolisa/blazed` | `root:root`, `0755` | Daemon-only server |
| `/run/blaze/api.sock` | service ownership, `0660` | 默认本地 API socket |
| `/etc/anolisa/blaze/config.toml` | 使用 `noreplace` 语义的 package config | Daemon 配置 |
| `/var/lib/blaze` | 保留的 state directory | Daemon 持有的 sandbox 状态 |

`blazed.service` 仍只启动 `blazed`。安装 `blazectl` 不会添加 client service、
改变 socket ownership 或启用 TCP。

### 升级与卸载

Upgrade 会替换两个版本匹配的 binary，同时保留 daemon 配置。Removal 删除
package-owned binary 和 service file，但不删除 `/var/lib/blaze` 下不属于
package 的 state。

```bash
sudo yum upgrade blaze
blazectl --version
/usr/libexec/anolisa/blazed --version
sudo yum remove blaze
```

### 从源码构建 client

构建必须在 Linux 上执行：

```bash
cd src/blaze
cargo build -p blazectl --release --locked
./target/release/blazectl version
```

使用从源码构建的 client 连接已安装的 daemon：

```bash
sudo ./target/release/blazectl --socket /run/blaze/api.sock list
```

不得在 macOS 或 Windows 上构建或测试 Blaze。

## Endpoint 选择

`blazectl` 按以下优先级选择 endpoint：

| 优先级 | 选择 | 结果 |
|-------:|------|------|
| 1 | `--socket <absolute-path>` | 通过所选 Unix socket 使用 HTTP |
| 1 | `--url <http-origin>` | 通过显式 TCP 使用 HTTP |
| 2 | `BLAZED_URL=<http-origin>` | 未提供 endpoint flag 时使用 TCP |
| 3 | 未提供 endpoint option | `/run/blaze/api.sock` |

`--socket` 与 `--url` 互斥。Endpoint flag 覆盖 `BLAZED_URL`。Socket path
必须是绝对路径。TCP URL 必须是纯 `http` origin，不能包含 user information、
path、query 或 fragment。选择 `--url` 不会启动或重新配置 daemon TCP listener。

```bash
sudo blazectl --socket /run/blaze/api.sock list
blazectl --url http://127.0.0.1:14159 list
BLAZED_URL=http://127.0.0.1:14159 blazectl list
```

TCP 不内置 authentication 或 TLS。只能绑定到已批准的可信管理网络，并在
Blaze 外部实施访问控制。

Client 固定边界：连接建立 deadline 为 5 seconds（秒），完整请求 deadline
为 30 seconds（秒），收集的 response 不得超过 32 MiB。Mutation request
不会自动重试。

## 输出与 Stream

成功输出按 `--output`、`BLAZECTL_OUTPUT`、`text` 的顺序解析。只接受
`text` 和 `json`。

```bash
sudo blazectl --output text list
sudo blazectl --output json list
sudo env BLAZECTL_OUTPUT=json blazectl list
```

- Text 成功输出使用稳定字段和确定性排序。
- JSON 成功输出向 stdout 写入恰好一个 JSON 值和一个换行。
- Runtime diagnostic 只写入 stderr，不污染 stdout。
- Text mode 下，`exec` 将 Guest stdout 转发到 stdout，将 Guest stderr
  转发到 stderr。JSON mode 下，成功时向 stdout 写入一个 response object，
  stderr 保持为空。
- Text `read` 将解码后的原始 bytes 写入 stdout，不做 UTF-8 转换，也不添加换行。
- JSON `read` 返回 `data_b64`，不会把任意 bytes 放入 JSON string。
- Error 不反射 endpoint 值、本地 input path、过大的 response body 或非结构化
  daemon response body。

## 命令参考

远程操作面恰好包含 14 个 canonical command：

| Command | HTTP request | 行为 |
|---------|--------------|------|
| `blazectl create [ID] [--template NAME]` | `POST /v1/sandboxes` | 创建 sandbox；`ID` 是可选 UUID |
| `blazectl exec <ID> <CMD> [--cwd PATH]` | `POST /v1/sandboxes/{id}/exec` | 将一个不透明 command string 传给 Guest Agent |
| `blazectl list` | `GET /v1/sandboxes` | 按稳定 ID 顺序列出 sandbox；alias：`ls` |
| `blazectl kill <ID>`<br>`blazectl kill --all` | `DELETE /v1/sandboxes/{id}`；`--all` 先使用 `GET /v1/sandboxes` | 销毁一个或全部已列出的 sandbox；单 ID alias：`rm` |
| `blazectl hibernate <ID>` | `POST /v1/sandboxes/{id}/hibernate` | Snapshot 并停止所选 backend |
| `blazectl checkpoint <ID>` | `POST /v1/sandboxes/{id}/checkpoint` | 提交新 checkpoint |
| `blazectl rollback <ID> <CHECKPOINT>` | `POST /v1/sandboxes/{id}/rollback/{checkpoint}` | 恢复 `ckpt-<uuid>` checkpoint |
| `blazectl checkpoints <ID>` | `GET /v1/sandboxes/{id}/checkpoints` | 列出 checkpoint 和 HEAD 可达性 |
| `blazectl prune-checkpoints <ID>` | `POST /v1/sandboxes/{id}/checkpoints/prune` | 删除 HEAD 不可达分支 |
| `blazectl resume <ID>` | `POST /v1/sandboxes/{id}/resume` | 恢复 hibernated backend |
| `blazectl cleanup-devices` | `POST /v1/pool/cleanup` | 排空 pool 资源并触发 refill |
| `blazectl pool-status` | `GET /v1/pool/status` | 显示 ready、capacity 和 pending pool 值 |
| `blazectl read <ID> <PATH>` | `POST /v1/sandboxes/{id}/read` | 读取一个绝对 Guest path |
| `blazectl write <ID> <PATH> [--file PATH\|-]` | `POST /v1/sandboxes/{id}/write` | 向一个绝对 Guest path 写入 binary data |

本地 version surface：

| Command | Daemon access | 结果 |
|---------|---------------|------|
| `blazectl version` | 无 | 稳定 text 或 JSON client version |
| `blazectl --version` | 无 | 标准本地 version line |

不存在 `blazectl` template、policy、hook、metrics、admin 或 daemon-lifecycle
command。

### Guest exec 与 binary file I/O

`exec` 从不调用本地 shell。调用者提供一个 `CMD` argument，client 将其作为
JSON data 传给 Guest Agent。内容包含空格时，应在调用 shell 中引用它。

`write` 按以下规则选择 input：

| Invocation | Input |
|------------|-------|
| `--file <path>` | 来自该本地文件的 binary bytes |
| `--file -` | 来自 stdin 的 binary bytes |
| 无 `--file`、stdin 非 terminal | 来自 stdin 的 binary bytes |
| 无 `--file`、stdin 是 terminal | 立即报错且不连接 daemon |

空 input 合法。超过 16 MiB 的 input 会在 transport 前被拒绝。`read` 和
`write` 在两种 output mode 下都保持 binary-safe。

```bash
ID=00000000-0000-4000-8000-000000000001
sudo blazectl create "$ID"
sudo blazectl exec "$ID" "printf sentinel"
printf '\000\001\002' > /tmp/blaze-sentinel.bin
sudo blazectl write "$ID" /tmp/sentinel.bin --file /tmp/blaze-sentinel.bin
sudo blazectl read "$ID" /tmp/sentinel.bin > /tmp/blaze-sentinel.out
cmp /tmp/blaze-sentinel.bin /tmp/blaze-sentinel.out
printf sentinel | sudo blazectl write "$ID" /tmp/sentinel.txt --file -
sudo blazectl kill "$ID"
```

### kill --all

`kill --all` 先获取稳定 sandbox ID 集合，再尝试每个 target，同时进行中的
delete request 不超过 50。一个 target 失败不会终止其他尝试。Summary 以
确定性顺序区分 `succeeded`、`failed`、`unfinished` 和 `total`；任何 failed
或 unfinished target 都返回 exit 1。全部成功的 summary 写入 stdout；
partial-failure summary 写入 stderr，并保持 stdout 为空。

## 退出码

| Exit | 含义 | Stream 契约 |
|-----:|------|-------------|
| 0 | 远程操作成功或本地 version 输出成功 | Result 写入 stdout；stderr 为空 |
| 1 | Endpoint、connection、protocol、daemon、input、cancellation、output 或 batch failure | stdout 不产生虚假成功；stderr 写入有界 diagnostic |
| 2 | CLI argument 无效或冲突 | Clap usage diagnostic 写入 stderr |
| 1–125 | Guest command 非零退出 | Text 保留 Guest stream；JSON 输出一个 response 值；process exit 与 Guest code 一致 |

Guest exit code 不在 1–125 时映射为 exit 1，并输出稳定 diagnostic。
`kill --all` 遇到 failed 或 unfinished target 时，会在尝试所有可能 target
后返回 exit 1。

## 限制与安全

- Blaze 仅支持 Linux x86_64 和 aarch64。
- 默认 `/run/blaze/api.sock` 使用 service ownership 和 `0660` mode，因此
  初始使用需要 service administrator/root 边界。
- `blazectl` 不启动 `blazed`、不修改 daemon 配置、不改变 socket permission，
  也不启用 TCP listener。
- TCP 是不内置 authentication 或 TLS 的纯 HTTP。
- Firecracker 操作需要合适的 Linux/KVM host，以及 daemon 配置的 mount、
  network namespace、tap 和 firewall 权限。
- 即使 daemon HTTP API 可能提供其他 endpoint，client 仍有意排除 template、
  policy、hook、metrics、admin 和 daemon-lifecycle 操作。

## 故障排查

- Socket 缺失或连接失败时，检查 `systemctl status blazed.service` 和所选
  endpoint。
- UDS permission error 应在已批准的 administrator/root 边界内处理；不要把
  socket 改成 world-writable。
- 机器处理应选择 `--output json`，并且只解析 stdout。
- 任何非 0 exit 都应视为失败；对于 `exec`，还需解释本文定义的 Guest
  exit-code 范围。
