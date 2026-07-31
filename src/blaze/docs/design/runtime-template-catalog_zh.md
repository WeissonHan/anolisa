# Runtime 模板目录

daemon 可以将一组可复用的 runtime artifact 发布到
`runtime_templates.dir` 配置的目录。只有同时配置
`runtime_templates.import_root` 后，导入功能才会启用。

该目录与已有的内存 `/v1/templates` registry 相互独立。它提供持久化发布和
查询，但 sandbox create 不会选择其中的条目。

## 导入请求

```http
POST /v1/runtime-templates/import
Content-Type: application/json

{
  "name": "runtime-base",
  "source": "runtime-base",
  "description": "base runtime"
}
```

`source` 是配置的导入根目录下的相对路径。绝对路径、父目录跳转和路径中的
符号链接都会被拒绝。每一级源目录和源文件都必须属于 daemon 用户，并且不能
允许 group 或其他用户写入。

源目录必须包含顶层普通文件 `vmstate.snap`、`mem.bin` 和 `rootfs.ext4`。
可选的 `template.json` 必须是 JSON object。嵌套目录、链接和特殊文件都会
被拒绝。daemon 会使用请求中的 `name`，采用请求中非空的 `description`，
并在缺少数值类型的 `rootfs_size` 或 `memory_size` 时填入默认值。目标名称
已存在或同名导入正在执行时返回 `409 Conflict`。

## 上限和目录边界

以下配置会在发布前限制一次导入所做的工作：

| 配置 | 含义 |
|------|------|
| `max_files` | 单个发布条目的文件数上限，包括 `template.json` |
| `max_bytes` | 单个条目的 artifact 与生成后元数据的总字节数上限 |
| `max_metadata_bytes` | 输入与生成后元数据的大小上限 |
| `max_total_bytes` | 已发布字节与并发预留字节之和的上限 |

`runtime_templates.dir` 与 `runtime_templates.import_root` 必须是绝对路径，
不能包含父目录组件，也不能互相重叠。它们还不能与存储镜像、存储实例或已有
模板配置目录重叠。

catalog、staging 目录和已发布目录的权限为 `0700`，已发布文件的权限为
`0600`。

## 发布与恢复

导入器打开源条目时不会跟随链接；它会先预留 catalog 容量，再把文件复制到
私有且名称唯一的 staging 目录。复制完成后会再次检查源文件的身份和大小。
完整目录同步后以不覆盖已有条目的方式改名发布，因此读取方只会看到“没有条目”
或完整条目。

导入失败会移除对应的 staging 目录。如果清理无法完成，或者条目已经发布但
catalog 持久性无法确认，daemon 会拒绝后续导入，直到修复 catalog 并重启。
启动时，daemon 会移除上次中断后遗留且归自己所有的 staging 目录，并校验
已发布条目的类型、所有者、权限、内容和容量。

正常关闭时，daemon 会拒绝新导入，请求取消正在执行的导入，等待相关文件句柄
和 staging 数据释放，然后继续既有的 runtime 清理。

## 查询与当前限制

已发布的元数据可通过以下接口查询：

- `GET /v1/runtime-templates`
- `GET /v1/runtime-templates/{name}`

列表按模板名称排序；已发布元数据损坏时会返回错误，而不是静默隐藏条目。这些
接口只管理已经保存的 artifact，校验范围仅限结构，不证明快照能够启动或与某个
backend 兼容。它不会让 sandbox create 自动选择导入模板，不会为导入条目维护
引用计数，也不会通过既有 `/v1/templates/gc` registry 路由删除其目录。
