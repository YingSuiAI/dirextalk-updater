# Dirextalk Updater

Dirextalk Updater 是独立运行在宿主机上的轻量 Go 服务。即使容器中的
message-server 正在停止、重启或升级，它仍可提供版本检查、升级进度、重启和
回滚控制。调用方不能传入任意 shell、Compose 路径、服务名、镜像地址或摘要，
这些高权限参数由 updater 代码和受保护配置固定维护。

第一版只支持 Ubuntu 24.04 `linux/amd64`。服务仅监听 Unix Socket，不开放独立
TCP 端口。message-server 通过挂载的 Socket 调用受 control token 保护的接口；
客户端只能拿到单个任务范围的 bearer，用于在主服务不可用时查询进度。

## 本地开发

需要 Go 1.24 或更高版本：

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
  "socket_path": "/run/dirextalk-updater/updater.sock",
  "control_token_file": "/etc/dirextalk-updater/control-token"
}
```

使用 `dirextalk-updater -config <路径> serve` 启动。运行状态采用限制权限、临时
文件、fsync 和原子替换持久化。v1 Unix API 前缀固定为
`/_dirextalk/updater/v1/`，包含版本发现、状态、期望状态、创建任务以及独立的
任务进度查询。版本兼容性和是否允许操作由服务端判断，客户端不能自行推断。

## 发布资产

稳定的 `vX.Y.Z` tag 会在 `ubuntu-24.04` 上执行验证，只构建一个
`linux/amd64` 二进制，并发布：

- `dirextalk-updater-linux-amd64`
- `dirextalk-updater-linux-amd64.sha256`
- `dirextalk-updater-release.json`

发布清单绑定版本、完整提交、构建时间、系统、架构、Ubuntu 版本、资产名和
SHA-256。安装程序应解析正式 tag Release 并校验清单与摘要，不能把可移动的
`latest` 下载地址当作不可变安装目标。
