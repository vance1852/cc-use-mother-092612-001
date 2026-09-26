# 构建中医夜市现场服务分流中枢基础服务

本项目提供中医文化夜市的通用后台基础能力，负责活动机构、服务站点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。新的业务模块可以在这些稳定边界之上增加领域状态、规则和接口。

`src/night_market_dispatch/` 是在基础层之上实现的现场服务分流中枢：

- 参与者的入场诉求、禁忌提示、已接受项目以行程事件持久化，可全程追溯；
- 区域容量与当班专家资格的每次变化推进场所状态版本，等候者去向可按版本重新计算；原区域仍可用时保持原队列，已筛查群众不因专家换岗被迫重新排队；
- 服务中的参与者不被重算移动，只能经由“发起—完成/取消”的明确交接转区；
- 高风险陈述一律转人工处理，系统只输出分流去向，不生成任何诊断内容；
- 叫号产生带超时的占用权，过号、暂停、恢复、转区由状态机按确定顺序生效（已叫号须先结算过号才能暂停，仅等候中可暂停，仅服务中可交接）；
- 写操作幂等：同一请求重放返回原决定，编号相同而内容不同报告冲突；
- 全部状态存于 SQLite，应用重启后已确认但未结束的流程（等候、已叫号、服务中、待交接）从持久化记录继续推进。

## 目录

- `src/night_market_foundation/`：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- `src/night_market_dispatch/`：分流中枢的领域服务、表结构、HTTP 路由和含重启恢复的离线验收；
- `tests/`：基础规则、事务边界、接口路由、分流状态机和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m night_market_foundation.acceptance
PYTHONPATH=src python3 -m night_market_dispatch.acceptance
```

验收命令会在临时 SQLite 数据库中登记活动机构、操作者、站点和参考资料，核对幂等回执与审计链，成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。分流中枢的验收额外覆盖：高风险转人工、叫号超时结算、完成项目后的去向接续、专家换岗后的重算、交接的发起与重启后完成。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m night_market_dispatch.api --database night_market.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。写入接口通过 `X-Actor-Id` 标识操作者，服务重启后 SQLite 中的业务状态和审计链继续保留。分流接口挂在 `/dispatch/` 下（区域、专家、入场、叫号、到场、完成、暂停、恢复、过号结算、重算、交接、人工处理），协调员查询接口为：

- `GET /dispatch/pressure?site_id=`：各区当前压力与状态版本；
- `GET /dispatch/participant?participant_id=`：参与者现状、当前去向所依据的状态版本、未完成的交接；
- `GET /dispatch/itinerary?participant_id=`：完整行程；
- `GET /dispatch/handovers/pending?site_id=`：尚未完成的交接。
