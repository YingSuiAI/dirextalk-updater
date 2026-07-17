# 中台版本直传升级（Updater）开发清单

本清单只覆盖 `dirextalk-updater`。移动端、中间服务和部署器的任务由各自仓库跟踪。

## 升级控制面

- [x] 删除 GitHub discovery 的实现、控制接口、CLI 入口和主动刷新路径。
- [x] 新增仅 Unix socket 可访问的 `/_dirextalk/updater/v1/control/status` 状态接口。
- [x] 新增 `/_dirextalk/updater/v1/control/jobs`，请求体仅接受 `target_version`、`idempotency_key`、`client_version` 和固定确认值。
- [x] `control/status` 暴露 `direct_contract_version: 2`；新增原子 replay-only 接口，未知 key 永不创建任务。
- [x] 严格拒绝 URL、镜像名、digest、命令、路径及其他未知字段。
- [x] 校验 canonical 稳定 SemVer（`vX.Y.Z`）、小写 UUID 幂等键和固定确认值。
- [x] 将同一幂等键绑定到同一目标版本，防止重放时改写升级目标。
- [x] 拒绝相等版本和降级版本，并维持单一活动任务限制。

## 镜像与执行链路

- [x] 将镜像仓库固定为 `dirextalk/message-server`，不接受调用方传入的仓库或镜像引用。
- [x] 从固定 GitHub 仓库最新正式 Release 校验 checksum-bound canonical release index 与嵌入 manifest，不信任可移动 tag。
- [x] 创建任务前校验当前固定源 digest、明确单跳 edge、schema/client 兼容性，并原子持久化 contract-v2 Plan 与 job。
- [x] 实现单跳直连升级：备份、激活 digest-pinned 镜像、健康检查、完成。
- [x] 保留原子状态写入、主机锁、恢复点、看门狗和失败后的内部自动恢复。
- [x] 保留 `rolled_back` 终态和公开 `restart`，移除公开手工 rollback 路由及操作。

## 兼容迁移

- [x] 将运行时状态升级至 schema v6。
- [x] 清理 schema v5 遗留的未引用 discovery/plan 数据；新直传任务只生成内部 contract-v2 Plan，不向调用方暴露 plan token。
- [x] 保留活动或历史 legacy job 所引用的 plan、token 和恢复点，保证旧任务仍可恢复执行。

## 文档与验证

- [x] 更新中英文运行文档，说明直传契约、固定仓库、digest 固定和 restart-only 行为。
- [x] 覆盖鉴权、严格 JSON、非法目标、降级、幂等、未知字段、公开 rollback 不可用等单元测试。
- [x] 覆盖 index/manifest/source/target digest 在执行前持久化、明确 edge、schema/client 门和 direct runtime 的单元测试。
- [x] 覆盖 active 到 `rolled_back` 竞争、terminal 同 key replay、未知 key 拒绝，以及 replacement bearer 轮换。
- [x] 覆盖 schema v5 迁移：清理 discovery/未引用 plan，并验证活动 legacy job 可继续自动恢复。
- [x] 运行常规 Go 测试、race、vet、模块校验和 Linux 构建冒烟测试。
- [ ] 在 Ubuntu Docker Compose fixture 上执行真实单跳升级及自动恢复集成测试（当前 Ubuntu 主机存在 `dirextalk-p2p` 容器资源；测试按保护逻辑跳过，不能复用可能在运行的服务）。
