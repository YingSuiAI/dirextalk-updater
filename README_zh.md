# Dirextalk Updater

Dirextalk Updater 是独立运行在宿主机上的轻量 Go 服务。即使容器中的
message-server 正在停止、重启或升级，它仍可提供版本检查、升级进度、重启和
自动恢复。中台只传入目标版本；updater 将其解析为固定仓库的
`dirextalk/message-server:<版本>` Docker tag。
调用方不能传入任意 shell、Compose 路径、服务名、镜像地址、摘要或 URL。

第一版支持 Ubuntu 22.04 和 24.04 `linux/amd64`。服务仅监听 Unix Socket，不开放独立
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

`serve` 启动前会校验宿主机，非 Ubuntu 22.04 或 24.04 `linux/amd64` 会直接
拒绝运行。`version` 是可在其他开发系统执行的只读命令。

## 运行配置

默认读取 `/etc/dirextalk-updater/config.json`：

```json
{
  "schema_version": 1,
  "state_dir": "/var/lib/dirextalk-updater",
  "socket_path": "/run/dirextalk-updater/http.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token",
  "caddy_mode": "compose"
}
```

必须以 root 使用 `dirextalk-updater -config <路径> serve` 启动；control token
必须由 root 所有且权限严格为 `0600`，非 root 运行会拒绝启动。运行状态采用
限制权限、临时文件、fsync 和原子替换持久化。v1 Unix API 前缀固定为
`/_dirextalk/updater/v1/`。`POST control/status` 只接受 `{}`，从宿主机配置的
canonical tag 或旧版 digest 固定镜像读取当前服务端版本，并返回
`current_version`、`updater_ready`、期望状态、活动
任务和 watchdog 状态。只有支持下述安全直传与 replay 合约的版本才会返回
`direct_contract_version: 2`。

`POST control/jobs` 只接受如下严格结构：

```json
{
  "target_version": "v1.0.3",
  "idempotency_key": "小写 UUID",
  "confirm": "apply_release_change"
}
```

目标必须是 canonical `vX.Y.Z` 且高于宿主机当前版本。鉴权后的 message-server
必须在调用 updater 之前完成中央版本授信。updater 不再访问 GitHub 或其他发布源，
不读取 release index/manifest，不要求 `upgrade_from`/前置版本边，也不接收或校验
目标 digest；它只构造 `dirextalk/message-server:<target_version>` 并把目标版本与
任务原子持久化。旧调用方可以继续发送可选 `client_version` 字段，但该字段不再是
升级门槛。
`upgrading` 只允许由创建任务的内部事务设置；存在活动任务时，外部不能覆盖任何
期望状态。

`POST control/jobs/replay` 只接受 `target_version` 与 `idempotency_key`，并在单个
原子状态事务中为同一已持久化 active 或 terminal job 轮换 replacement bearer。
未知 key 返回 HTTP 404 与 `idempotency_not_found`，绝不创建任务；同 key 不同目标
返回 HTTP 409。replay 不依赖当前 Release 可用性或 Plan 是否过期。
配置文件同样必须由 root 所有、权限严格为 `0600`，且必须是非符号链接的普通
文件。
`caddy_mode` 仅允许固定枚举 `compose` 或 `systemd`，旧配置缺省为 `compose`；
systemd 模式只能操作固定的 `caddy.service`。该值只来自 root-owned 配置，API
不能传入或覆盖。
`compose_project` 可省略，默认使用 `dirextalk-p2p`。另一个允许值仅为代码固定的
`dirextalk-message-server` deployer 管理布局，供当前安装和兼容迁移使用；它只能由
root 配置，控制 API 不能传入。
已持久化的 legacy release-index Plan 与 digest 事实仅保留给已有中断任务继续执行
或完成自动恢复；新的 contract-v2 任务不会创建 Plan，也不会执行发布发现。
受支持的已采用 `v0.15.2` 源仍会把明确的 legacy health/恢复假设写入备份元数据。

任务在执行任何宿主机变更前都会先持久化检查点。updater 会短暂停止
message-server，生成一致的 PostgreSQL custom dump、message 配置/数据归档和
宿主机 `p2p` 归档；校验文件摘要、源版本、镜像摘要和 schema 元数据后，才会把
staging 目录原子替换为 `backup/current`。始终只保留这一份已提交恢复点，损坏
的 staging 备份不会覆盖它。

目标服务按 `dirextalk/message-server:vX.Y.Z` 拉取和重建。升级成功必须连续确认
配置与运行中的 canonical tag、服务报告版本、PostgreSQL、内部健康接口以及同域
Caddy 健康接口完全一致。源镜像 digest 只允许在本机捕获以保留精确回滚点，不作为
目标授信依据；失败会
自动恢复，updater 自身重启后会从已持久化步骤继续。连续三次恢复失败后任务进入
maintenance，只会通过带 job bearer 的
`POST /_dirextalk/updater/v1/jobs/{job_id}/restart` 暴露 `restart` 操作。不会公开
手工 rollback；内部自动恢复仍保留，接口不接受任何基础设施参数。

常驻进程还会通过 Docker 故障事件和每 30 秒一次的对账监测固定 Compose 项目。
只有持久化期望状态为 `running` 时才允许自愈；连续观察到三次异常后才执行修复，
十分钟内最多尝试三次，预算耗尽后进入十五分钟降级冷却。修复严格按 Docker、
PostgreSQL、message-server、Caddy 顺序启动，并只使用当前已配置且本机存在的
canonical tag 或旧版 digest 固定镜像；不会解析 Release、拉取 `latest`、轮换备份
或执行迁移。
systemd 模式仍在固定 Compose project 中管理 PostgreSQL 与 message-server，
Caddy 观察和修复只使用 `caddy.service`。

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
