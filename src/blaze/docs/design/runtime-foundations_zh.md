# Runtime 基础设计

[English](runtime-foundations.md)

Blaze 通过与后端无关的 runtime contract 持有每个 sandbox。daemon 选择一个
`BackendSpawner`，取得其返回的 `BackendInstance`，并持有该 handle，直到
sandbox 被销毁或进程退出。后端特定的启动、暂停、snapshot、restore 和清理
行为均保留在这些 trait 后面，使生命周期层能在后续步骤失败时补偿已经完成的
步骤。

## 进程持有与恢复

真实后端进程会收到 `BLAZE_INSTANCE_ID`，并在实例 run directory 中写入
后端专用的 PID 文件。正常关闭先请求 `SIGTERM`，等待五秒；仅当进程仍未退出
时才使用 `SIGKILL`。重启清理会读取已记录进程的环境，并且只在实例标识匹配时
执行。若无法读取该环境，清理会停止并保留进程，供运维人员检查。

成功清理后会删除后端 socket 和 PID 元数据。serial log 在 16 MiB 时轮转。
destroy 后会有意保留 run directory 中剩余的日志和配置，供问题诊断使用。

## File storage provider

file provider 将不可变 image 与可变实例 slot 放在互不重叠的目录中。配置加载
会拒绝相同或互为父子目录的 root；daemon 启动时还会在路径规范化后再次检查。

每个已分配 slot 都持有独立的 root filesystem 和 memory 文件。存在基础产物
时，provider 会将它们复制进 slot。完整复制会使用更多容量并增加 create
延迟，但每个 slot 和恢复用 snapshot 都是自包含的：删除或修改其他 slot 不会
使它失效。未来可以通过 `StorageProvider` trait 增加支持 copy-on-write 或
content-addressed sharing 的 provider，同时保持相同的独立 restore contract。

持久化的生命周期记录只包含稳定的实例标识，不包含 provider 路径。daemon
重启后，由当前配置的 provider 在自己的 root 下重新构造全部路径。

## 可选 VM 网络

只有 policy 明确启用时才创建 VM 网络。启用该能力需要 `ip`、`unshare`
可执行文件以及足够的主机权限。Blaze 会分配独立 namespace 和 link pair，
并跳过主机上已经存在的名称。请求中的 interface identifier 会一致用于 VM
配置和 boot arguments。

namespace 设置为 guest 提供通过主机的出站连接。主机路由、转发策略、DNS
和上游连通性由运维人员负责；Blaze 不修改主机的全局转发设置。

setup 使用补偿式清理：任一步骤失败时，只删除本次尝试已经创建的资源。已有
namespace 不会被预先删除；检测到并发占用后会改用另一个 slot 重试。
