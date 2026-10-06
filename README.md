# device-twin-service-go

一套本地可运行的设备影子（Device Twin / Shadow）服务。用本地函数调用模拟平台与设备之间的连接与收发，不依赖任何外部服务或真实硬件，并保证在并发更新、断线重连与进程重启后，期望状态、上报状态与离线积压指令都能得到一致且可解释的结果。

## 结构

- `shadow/`：版本化影子文档。期望（desired）与上报（reported）分节维护，提供 CAS 更新、合并语义与差异计算。
- `twin/`：设备影子服务。差异下发、离线指令队列、连接模拟（Connect/Disconnect/Ack）、磁盘持久化与重启恢复。

## 影子文档语义

每台设备一份文档：

```json
{
  "version": 3,
  "desired":  { "light": "on", "config": { "interval": 10 } },
  "reported": { "light": "off" }
}
```

- `desired` 与 `reported` 完全独立，分别通过 `UpdateDesired` / `UpdateReported` 更新。
- 更新采用 JSON Merge Patch（RFC 7386）语义，规则确定且稳定：
  - 值为 `null`：删除该字段；
  - 双方都是对象：递归合并；
  - 其他情况：整体替换（数组不合并）。
- 相同的更新序列重复执行必然得到相同文档（有单测保证）。

## 版本与冲突规则

- 文档持有单调递增的 `version`，每次成功写入 +1，重启后继续递增、绝不回退。
- 每次写入必须携带其基于的 `baseVersion`（CAS）。不匹配时写入被明确拒绝，返回 `*shadow.ConflictError`（可用 `shadow.IsConflict` 判定），其中携带 `Section`、`Base`、`Current`，原因可区分；被拒绝的写入不会污染状态。
- 所有状态变更在单把互斥锁下串行提交，并发下不会丢写，也不会出现旧值覆盖新值。

## 增量下发与离线指令投递保证

- 每次 desired 更新后计算 `Diff(desired, reported)` 生成增量，进入该设备的保序 outbox（单调 `seq`）。
- 设备离线期间，增量与指令按设备保序积压；`Connect` 时按 seq 顺序补齐并标记 `sent`。
- 去重与有限重发：
  - 已连接时重复 `Connect` 返回 `ErrAlreadyConnected`，不重复下发；
  - 已 `sent` 未 `ack` 的条目在断连/重启后回到 `pending`，重连后重发，但每条最多投递 `WithMaxAttempts` 次（默认 3），超限标记 `expired`，不无限重发也不静默丢弃；
  - 被更新写入覆盖的增量标记 `superseded`，只下发最新差异；
  - 上报状态已满足期望时，未投递增量直接 `superseded`。
- `Ack` 的结果可区分：`applied` / `duplicate` / `superseded` / `expired` / `unknown`，迟到或重复的确认是可解释的 no-op。

## 持久化与重启恢复

- 每台设备的状态（文档版本、outbox、seq 计数器）以 JSON 快照写入数据目录，采用临时文件 + rename 的原子写，崩溃不会留下半更新状态。
- `Open` 在返回前完成全部恢复，恢复期间不对外暴露任何中间状态；崩溃时处于 `sent` 的条目恢复为 `pending` 等待补发。
- 单台设备或单条连接的异常只影响自身；并发场景由全局串行提交保证每台设备的最终状态确定且可解释。

## 本地验证

```bash
# 竞态检测下运行全部单测（需要 CGO，日志会列出每个业务场景及用例结果）
CGO_ENABLED=1 go test -race ./... -v
```

测试不依赖真实等待与真实网络，全部通过本地调用模拟。覆盖场景：版本冲突（含并发写）、字段删除与嵌套合并、更新序列确定性、差异计算、离线补发顺序、重复连接去重、覆盖（superseded）、有限重发（expired）、重启恢复、多设备并发隔离。
