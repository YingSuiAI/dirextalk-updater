# Dirextalk Updater

Dirextalk Updater 是独立运行在宿主机上的轻量 Go 服务。即使容器中的
message-server 正在停止、重启或升级，它仍可提供版本检查、升级进度、重启和
自动恢复。中台只传入目标版本；updater 将其解析为固定仓库的 Docker digest。
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
`/_dirextalk/updater/v1/`。`POST control/status` 只接受 `{}`，从宿主机固定镜像
读取当前服务端版本，并返回 `current_version`、`updater_ready`、期望状态、活动
任务和 watchdog 状态。只有支持下述安全直传与 replay 合约的版本才会返回
`direct_contract_version: 2`。

`POST control/jobs` 只接受如下严格结构：

```json
{
  "target_version": "v1.0.3",
  "idempotency_key": "小写 UUID",
  "client_version": "v1.0.0",
  "confirm": "apply_release_change"
}
```

目标必须是 canonical `vX.Y.Z` 且高于宿主机当前版本。每个新幂等键都会从固定
GitHub 仓库获取最新正式稳定 Release，限制响应大小与 HTTPS 重定向目标，校验
`release-index.json.sha256`，并严格验证 canonical index 及其中 manifest 的摘要。
随后必须找到一条明确的“源版本+源 digest -> 目标版本”单跳边，核对宿主机当前
固定镜像 digest、完整 schema 健康信息，以及调用方提供的客户端版本范围；可移动
tag 不能作为升级信任根。

message-server 发布工作流只有在每个源 digest 的 retained-data attestation 及其
checksum 验证通过后才生成并发布 canonical index。因此，正式 Release 中受
checksum 绑定的 index 是运行时的 attestation-derived 升级边权威；updater 仍会
独立复核 index、目标 manifest 和实际源 digest，再把这些摘要及兼容性事实作为
contract-v2 单跳 Plan 原子持久化。不会把间接路径悄悄转换为直连升级。
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
已持久化的 legacy plan 仅保留给已有中断任务完成自动恢复。contract v2 只会在
鉴权后的新任务创建流程内生成内部 Plan，不暴露 discovery 或 plan-token API。
不会再创建新的 legacy GitHub-discovered plan。受支持的已采用 `v0.15.2` 源只有在
匹配代码批准的固定镜像 digest 时才能进入直升链路，其明确的 legacy health/恢复
假设会写入备份元数据。

任务在执行任何宿主机变更前都会先持久化检查点。updater 会短暂停止
message-server，生成一致的 PostgreSQL custom dump、message 配置/数据归档和
宿主机 `p2p` 归档；校验文件摘要、源版本、镜像摘要和 schema 元数据后，才会把
staging 目录原子替换为 `backup/current`。始终只保留这一份已提交恢复点，损坏
的 staging 备份不会覆盖它。

目标服务只允许按 `vX.Y.Z@sha256:...` 拉取和重建。升级成功必须连续确认运行
容器摘要、PostgreSQL、内部健康接口以及同域 Caddy 健康接口完全一致；失败会
自动恢复，updater 自身重启后会从已持久化步骤继续。连续三次恢复失败后任务进入
maintenance，只会通过带 job bearer 的
`POST /_dirextalk/updater/v1/jobs/{job_id}/restart` 暴露 `restart` 操作。不会公开
手工 rollback；内部自动恢复仍保留，接口不接受任何基础设施参数。

常驻进程还会通过 Docker 故障事件和每 30 秒一次的对账监测固定 Compose 项目。
只有持久化期望状态为 `running` 时才允许自愈；连续观察到三次异常后才执行修复，
十分钟内最多尝试三次，预算耗尽后进入十五分钟降级冷却。修复严格按 Docker、
PostgreSQL、message-server、Caddy 顺序启动，并只使用当前已配置且本机存在的
tag+digest 镜像；不会解析 Release、拉取 `latest`、轮换备份或执行迁移。
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
