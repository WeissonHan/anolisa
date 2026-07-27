# Sandbox 管理 API

[English](management-api.md)

Blaze 通过 daemon HTTP 服务提供 sandbox 操作。规范资源路径是
`/v1/sandboxes`；现有 `/v1/instances` 路由作为兼容别名，调用同一个
`SandboxManager`。因此持久化状态、provider slot 与 backend handle 只有
一个持有者。

## 请求边界

daemon 通过 Unix domain socket 接收请求，也可以按配置启用 TCP listener。
每个请求最多收集 `api.max_body_bytes` 字节。guest 读写还受
`api.max_file_bytes` 限制。`api.request_timeout` 限制一次 guest 操作，且
必须至少为 11 秒，从而为命令执行保留协议完成时间。

错误是包含四个稳定字段的 JSON object：

```json
{
  "code": "not_found",
  "message": "not found: sandbox",
  "operation": "GET /v1/sandboxes/00000000-0000-0000-0000-000000000000",
  "sandbox_id": "00000000-0000-0000-0000-000000000000"
}
```

路由不包含有效 UUID 时，`sandbox_id` 为 `null`。

## Sandbox 操作

`POST /v1/sandboxes` 接收 `workload_class`、`image_digest`、labels、可选
template name，以及可选的调用方指定 UUID。manager 创建或领取资源前，
policy evaluation 会选择 backend 与 runtime 设置。

guest 命令执行接收 `cmd`、可选 `cwd` 和环境变量，以及可选的秒级 timeout。
read 请求包含绝对路径 `path`。write 请求包含绝对路径 `path` 和标准
base64 编码的 `data_b64`。

checkpoint、rollback、hibernate 与 resume 使用
[Recovery 事务](recovery-transactions_zh.md)描述的持久化事务。API 不会复制
它们的持有关系，也不会维护第二份 lifecycle map。

## 重试行为

- 以相同的调用方指定 UUID 和相同不可变参数重复 create，会返回现有 running
  sandbox；参数不一致时返回 `409`。
- sandbox 进入终止状态后，destroy 保持幂等。
- 每次成功的 checkpoint 请求都会创建新的 checkpoint ID。
- 可以重复 rollback 到同一个有效 checkpoint。
- hibernate 与 resume 要求各自的源状态；成功转换后重复调用会返回 conflict。

服务不解析 idempotency-key header。需要稳定 create 重试键的调用方应提供
UUID。

## 模板导入

`POST /v1/templates/import` 接收：

```json
{
  "name": "runtime-base",
  "source_dir": "/var/lib/blaze/import/runtime-base",
  "description": "base runtime"
}
```

源目录必须包含 `vmstate.snap`、`mem.bin` 和 `rootfs.ext4`。
`template.json` 可选；缺少时 importer 会创建它，并统一 name，补充默认
`rootfs_size` 和 `memory_size`。文件先复制到唯一 staging directory，校验
完成后再通过 rename 发布。嵌套目录与 symbolic link 会被忽略。

## 持有关系与关闭顺序

daemon 在接收请求前协调持久化记录。所有管理路由共享同一个 manager，因此
兼容别名不会与规范状态分离。关闭时先停止 listener，再由 manager 排空 warm
资源并清理由其持有的 runtime。

客户端命令行工具不属于该 API 服务。
