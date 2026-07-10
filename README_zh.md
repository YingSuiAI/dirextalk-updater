# Dirextalk Updater

Dirextalk Updater 是独立运行在宿主机上的轻量 Go 服务。即使容器中的
message-server 正在停止、重启或升级，它仍可提供版本检查、升级进度、重启和
回滚控制。调用方不能传入任意 shell、Compose 路径、服务名、镜像地址或摘要，
这些高权限参数由 updater 代码和受保护配置固定维护。

第一版只支持 Ubuntu 24.04 `linux/amd64`。服务仅监听 Unix Socket，不开放独立
TCP 端口。message-server 通过挂载的 Socket 调用受 control token 保护的接口；
客户端只能拿到单个任务范围的 bearer，用于在主服务不可用时查询进度。

## 本地开发

普通开发需要 Go 1.24 或更高版本；正式发布固定使用 Go 1.24.13：

```text
go test ./...
go test -race ./...
go vet ./...
go mod verify
```

普通本地构建固定显示 `v0.0.0-dev+local`：

```text
go build -o dirextalk-updater ./cmd/dirextalk-updater
./dirextalk-updater version
```

`serve` 和 `trigger-discovery` 启动前会校验宿主机，非 Ubuntu 24.04
`linux/amd64` 会直接拒绝运行。`version` 和 `resolve-release` 是只读命令，可用于
其他开发系统上的构建检查。

## 运行配置

默认读取 `/etc/dirextalk-updater/config.json`：

```json
{
  "schema_version": 1,
  "state_dir": "/var/lib/dirextalk-updater",
  "socket_path": "/run/dirextalk-updater/http.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token"
}
```

必须以 root 使用 `dirextalk-updater -config <路径> serve` 启动；control token
必须由 root 所有且权限严格为 `0600`，非 root 运行会拒绝启动。运行状态采用
限制权限、临时文件、fsync 和原子替换持久化。v1 Unix API 前缀固定为
`/_dirextalk/updater/v1/`，包含版本发现、状态、期望状态、创建任务以及独立的
任务进度查询。版本兼容性和是否允许操作由服务端判断，客户端不能自行推断。
版本发现结果最多保持 36 小时 fresh；过期、未来或缺少检查时间都会按 stale
处理，不能生成升级计划。`upgrading` 只允许由创建任务的内部事务设置；存在
活动任务时，外部不能覆盖任何期望状态。

任务在执行任何宿主机变更前都会先持久化检查点。updater 会短暂停止
message-server，生成一致的 PostgreSQL custom dump、message 配置/数据归档和
宿主机 `p2p` 归档；校验文件摘要、源版本、镜像摘要和 schema 元数据后，才会把
staging 目录原子替换为 `backup/current`。始终只保留这一份已提交恢复点，损坏
的 staging 备份不会覆盖它。

目标服务只允许按 `vX.Y.Z@sha256:...` 拉取和重建。升级成功必须连续确认运行
容器摘要、PostgreSQL、内部健康接口、schema 元数据以及同域 Caddy 健康接口
完全一致；失败会自动回滚，updater 自身重启后会从已持久化步骤继续。连续三次
恢复失败后任务进入 maintenance，只会通过带 job bearer 的
`POST /_dirextalk/updater/v1/jobs/{job_id}/{operation}` 暴露已持久化的
`rollback`/`restart` 操作，接口不接受任何基础设施参数。

常驻进程还会通过 Docker 故障事件和每 30 秒一次的对账监测固定 Compose 项目。
只有持久化期望状态为 `running` 时才允许自愈；连续观察到三次异常后才执行修复，
十分钟内最多尝试三次，预算耗尽后进入十五分钟降级冷却。修复严格按 Docker、
PostgreSQL、message-server、Caddy 顺序启动，并只使用当前已配置且本机存在的
tag+digest 镜像；不会解析 Release、拉取 `latest`、轮换备份或执行迁移。

## 发布资产

稳定的 `vX.Y.Z` tag 会在 `ubuntu-24.04` 上使用固定 Go 1.24.13 执行验证，
通过两个独立缓存连续构建并要求结果逐字节一致，只发布一个 `linux/amd64`
二进制及其元数据：

- `dirextalk-updater-linux-amd64`
- `dirextalk-updater-linux-amd64.sha256`
- `dirextalk-updater-release.json`

发布清单绑定版本、完整提交、构建时间、系统、架构、Ubuntu 版本、资产名和
SHA-256。安装程序应解析正式 tag Release 并校验清单与摘要，不能把可移动的
`latest` 下载地址当作不可变安装目标。

构建时间固定取 tag 所指提交的 UTC commit timestamp，而不是 runner 当前时间。
在 Ubuntu 24.04 `linux/amd64` 上该提交的干净工作区，可使用与 CI 相同的
契约预计算摘要：

```text
VERSION=v1.0.0 COMMIT=<完整提交> BUILD_TIME=<提交UTC时间> scripts/build-release.sh
```
