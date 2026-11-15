# 本地资料源与调度状态约定

源端以明确注册信息给出逻辑站点到物理通道的可共享证据。`src/transport.mjs` 提供真实本地 h2c 测试通道；设置变化通过 HTTP/2 SETTINGS 可观察，流取消后延迟发出源端释放事件。接收侧由 `src/pool.mjs` 接入该传输接口，模拟拓扑本身不参与配额或排队判断。

## 通用控制

`GET /api/snapshot` 查看当前原始源状态；`POST /api/action` 接收 JSON 对象 `{"action":"动作名"}`。可用动作：reset、shrink、drain。控制只改变模拟源，不代表接收器已经处理了变化。

`npm start` 还会启动本地复用通道，并在启动输出中给出地址和容量；`CHANNEL_CAPACITY` 只改变初始容量，运行中的容量变化应通过传输接口的 SETTINGS 或测试里的 `resize()` 产生。

所有资料均由本项目构造，包含的机构、设备、会话和文件仅供本地使用，不访问真实账号。浏览器操作应通过正常可见界面；测试用源控制接口不是业务采集捷径。

## 连接信息

站点到通道的归属只来自 `fixtures/source.json` 中的显式登记：

- `channels[].aliases` 列出使用该通道的站点；出现两个及以上站点时 `proof` 必填。
- 只有地址或名称相同不构成共享证据；一个通道上出现第二个站点却缺少 `proof` 时，资源池拒绝建立归属。独占通道不需要共享证明。
- 同一通道 id 只对应一个会话；若连接信息给出不同地址，视为错误而不是默默复用。
- 别名迁移通过 `RegistryBook.migrate`（或 `TransportPool.setBinding`）显式进行，并记录原因。
- 排队作业在通道容量变化、别名迁移、GOAWAY/drain、会话关闭时重新评估，不沿用入队时的判断。

## 调度状态

| 状态 | 含义 | 是否占用站点额度 | 是否占用通道窗口 |
| --- | --- | --- | --- |
| `queued` | 等待明确的连接信息或可用窗口 | 否 | 否 |
| `dispatching` / `running` | 已授权并在读响应 | 是 | 是 |
| `paused` | 调用方暂停读取大响应 | 是 | 是 |
| `cancelling` | 已发 RST，等待本地流结束 | 是 | 是 |
| `releasing` | 本地流结束，等待供方释放确认 | 是 | 是（`awaitingRelease`） |
| `completed` | 响应自然读完 | 否 | 否 |
| `cancelled` / `released` / `failed` | 取消、暂停后被关闭、或响应中断 | 否 | 否 |

`TransportPool.snapshot()` 同时给出逻辑侧（`sites[].held/limit/queued`）和物理侧（`channels[].wire/freeWindow/awaitingRelease`），`invariant` 是同一份快照的自检。`pressure.overCapacityGrants` 统计任何超出通道容量的新授权，正常应恒为 0。

暂停读取会持续占用额度与窗口。需要时不设 `pausedTimeoutMs` 就会一直占用；设置后超时会走真实取消，再经 `releasing` 到 `cancelled`，不会被记成完成。

## 来源证据

源端请求记录只用于核查服务行为，接收器自身的审计、状态和输出需由扩展功能负责。源端重启会恢复默认资料，但本地接收文件不会自动清空。
