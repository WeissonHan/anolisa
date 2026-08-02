# 变更日志

[English](CHANGELOG.md)

本文件记录 ANOLISA Blaze 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [0.4.0] - 2026-08-01

### 新增

- 暂停与恢复运行中的 sandbox：`POST /v1/instances/{id}/pause` 与 `/resume`。
- 将 sandbox 快照写入持久化快照库：`POST /v1/instances/{id}/snapshot` 默认保持 sandbox
  继续运行，传 `{"leave_running": false}` 则休眠。
- 用 `POST /v1/instances/{id}/restore` 原地恢复已休眠的 sandbox；用
  `POST /v1/snapshots/{id}/restore` 从任意快照孵化全新实例。快照的生命周期长于产生它的
  实例，因此同一镜像可反复恢复，源实例销毁后依然可用。
- 通过 `GET /v1/snapshots`、`GET /v1/snapshots/{id}` 与 `DELETE /v1/snapshots/{id}`
  查看和回收快照。若某镜像是休眠 sandbox 唯一的恢复途径，删除会被拒绝而不会将其搁死。
- gVisor 后端实现上述四个操作；策略中的 `[checkpoint].enabled = false` 现在会真正拒绝快照。
- 新增可选的 `[containerd]` 配置段。配置后，gVisor 后端不再共用
  `<images_dir>/gvisor-rootfs` 这份需要手工准备的只读目录，而是让每个 sandbox 根植于一个
  普通 OCI 镜像并拥有自己的可写层。这里只借用 containerd 的镜像与快照服务，sandbox 进程
  与完整生命周期仍归 blaze 持有。不写该段则继续使用共享基础镜像。
- 创建请求支持 `image` 镜像引用，例如 `{"image": "docker.io/library/alpine:latest"}`。
  `image_digest` 仍是策略匹配与 warm pool 配对所用的负载身份，`image` 则是后端准备文件
  系统时的定位符。快照会记录它，因此孵化能在全新的运行目录里构建出一致的文件系统。
- 新增计数器：`blaze_instances_paused_total`、`blaze_instances_resumed_total`、
  `blaze_instances_restored_total`、`blaze_instances_hatched_total`、
  `blaze_snapshots_created_total`、`blaze_snapshots_failed_total`、
  `blaze_snapshots_deleted_total`。

### 变更

- **不兼容变更** `POST /v1/instances/{id}/checkpoint` 不再只是移动状态机，而会真正执行
  快照。它仍然会休眠实例，但现在可能失败（后端不支持快照返回 501、守护进程不持有后端
  所有者返回 409、保存失败返回 500），且 `checkpoint_id` 由 `ckpt-<uuid>-<ts>` 变为可在
  `/v1/snapshots` 中查询的快照 uuid。
- `start_path` 新增第三种取值 `restored`，用于从快照启动的实例。恢复后写入的实例状态
  无法被 0.3.x 读取。

### 修复

- gVisor sandbox 现在通过 `state_dir` 下显式的 runsc 状态根目录寻址，因此守护进程即使在
  不同环境下重启，仍能管理和回收自己启动的 sandbox。
- 创建 gVisor 实例时不再在 sandbox 就绪前就报告为 running，此前会导致紧随其后的操作出现
  假失败。

## [0.3.0] - 2026-07-22

### 新增

- 通用 `StorageProvider` trait，支持可插拔存储后端架构。
- `FileStorageProvider`：默认文件存储后端，适用于开发和标准部署。
- `[storage]` 配置段：`provider`、`pool_size`、`prefork`、`flush_interval` 字段，均有向后兼容的默认值。
- `GET /v1/health` 现返回 `storage_pool` 状态（ready/capacity/pending）。

## [0.2.1] - 2026-07-21

### 变更

- **品牌重塑**：组件从 Anvil 更名为 Blaze。二进制：`blazed`，配置路径：`/etc/anolisa/blaze/`，状态目录：`/var/lib/blaze/`。
- Firecracker vCPU 配置现已校验上限（1–32）。

### 新增

- 组件已注册到项目清单（根 README、AGENTS.md、PR 模板）。
- VM 资源配置回退链已在 README 中说明。

## [0.2.0] - 2026-06-30

### 新增

- FirecrackerSpawner：支持 Firecracker microVM 后端，daemon 启动时自动探测并选择最强隔离。
- TCP 远程 API：可配置 `[listen].http_addr` 开启 TCP 监听（端口 14159），供平台远程调用。
- 优先级后端选择：`build_spawner()` 按 firecracker → linux-sandbox → mock 优先级自动选型。
- Storage section：`[storage].images_dir` 统一管理 vmlinux/rootfs 查找路径。
- 打包骨架：`dist/anvil.service`（systemd unit）+ `anvil.spec`（RPM）+ `tmpfiles-anvil.conf`。
- `[backends]` 配置段，直接映射后端二进制路径。

## [0.1.3] - 2026-06-24

### 变更

- sandbox 进程现在运行在完整 namespace 隔离中（PID、网络、文件系统）。

## [0.1.2] - 2026-06-22

### 新增

- daemon 现在管理 sandbox 进程生命周期：创建时自动启动，销毁时自动终止。
- backend 二进制不可用时优雅降级（便于开发环境使用）。

## [0.1.1] - 2026-06-20

### 新增

- Policy 校验在 sandbox 启动前拒绝不安全的配置。
- 与 `osbase sandbox uninstall` 安全协调（防止移除正在使用的 backend）。

## [0.1.0] - 2026-06-18

ANOLISA Anvil 首个骨架版本。

### 新增

- 通过 HTTP API 创建、列出、查看、checkpoint（仅状态转换）、reset、销毁 sandbox。
- 策略驱动的 backend 选型：指定 workload class 即可自动匹配合适的 sandbox 类型。
- Warm pool：预创建 sandbox 随时分配，可配置 min/target/max 容量。
- 模板共享：多个 sandbox 共用一份 base 内存镜像，降低单实例内存开销。
- Prometheus metrics 端点，供监控系统采集。
