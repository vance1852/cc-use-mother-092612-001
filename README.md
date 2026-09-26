# 构建中医夜市现场服务分流中枢基础服务

本项目提供中医文化夜市的通用后台基础能力，负责活动机构、服务站点、操作者和结构化参考资料的登记，内置角色权限、请求幂等、SQLite 事务与哈希串联审计。新的业务模块可以在这些稳定边界之上增加领域状态、规则和接口。

`src/night_market_foundation/triage/` 在基础层之上实现**服务分流中枢**：保存参与者入场诉求、禁忌提示与已接受项目的可追溯行程，登记各区域容量与当班专家资格，依据单调状态版本给出可重放的分流决定；叫号占用超时自动释放，暂停、恢复、过号重呼和改派按确定顺序生效，服务中转区必须经过明确交接，高风险陈述一律转人工，系统不生成诊断。

## 目录

- `src/night_market_foundation/`：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- `src/night_market_foundation/triage/`：区域/专家排班、风险筛查与分流、叫号过号、改派与交接、协调员查询和重启恢复；
- `tests/`：基础规则、事务边界、接口路由、分流规则和端到端验收测试。

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
PYTHONPATH=src python3 -m night_market_foundation.triage.acceptance
```

验收命令会在临时 SQLite 数据库中登记活动机构、操作者、站点和参考资料，核对幂等回执与审计链，成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。分流验收额外覆盖三区排班、高风险转人工、禁忌回避、叫号超时与过号重呼、专家换岗改派、服务中交接、暂停/恢复、压力查询以及跨重启的流程续推。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m night_market_foundation.api --database night_market.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。写入接口通过 `X-Actor-Id` 标识操作者，服务重启后 SQLite 中的业务状态和审计链继续保留，启动时会自动执行一次恢复，补齐停机期间到期的叫号占用。

分流接口统一以 `/triage` 开头，主要包括：

- 配置：`POST /triage/zones`、`POST /triage/zones/capacity`、`POST /triage/zones/suspend`、`POST /triage/zones/resume`、`POST /triage/experts`、`POST /triage/experts/update`；
- 入场与分流：`POST /triage/participants/intake`、`POST /triage/participants/review`、`POST /triage/route`、`POST /triage/reassign`；
- 叫号：`POST /triage/call-next`、`POST /triage/check-in`、`POST /triage/finish-service`、`POST /triage/expire-tickets`；
- 交接：`POST /triage/handshakes/request|confirm|complete|cancel`；
- 查询：`GET /triage/zone-pressures`、`GET /triage/handshakes`、`GET /triage/participant`、`GET /triage/journey`、`GET /triage/state-version`；
- 恢复：`POST /triage/recover`。

所有写接口都要求 `request_id`：相同编号与内容的重放返回同一决定（HTTP 200），编号相同而内容不同返回 409 冲突。

## 分流规则要点

- **高风险转人工**：入场陈述包含胸痛、呼吸困难、妊娠等高风险编码时，参与者进入 `manual_review`，系统不会自动分流，须由协调员通过 `review` 人工处理；系统只按结构化编码路由，不生成诊断。
- **禁忌回避**：禁忌编码映射到不兼容服务类型（如皮肤损伤回避推拿），路由选择只在兼容且有合格当班专家的运行中区域内进行。
- **叫号占用**：叫号生成带 `expires_at` 的占用，超时或暂停宽限到期后由确定性扫描释放（过号原因 `missed`/`suspended_timeout`），过号票按 `miss_seq` 优先重呼；超过两次过号票终止，群众无需重新筛查即可再次分流。
- **改派与交接**：容量或专家变化后 `reassign` 只移动等待中的票并保留原始入队时间；`called`/`serving` 的占用不被自动改派，服务中者只能经 `request → confirm → check-in → complete` 的明确交接转区，交接确认时重新校验目标区状态、专家资格与服务位。
- **可追溯**：每位参与者有 append-only 行程事件流，每个事件记录当时的状态版本；每次配置变更推进单调 `state_version`，改派决定同时保存 `basis_version` 与生效版本。
- **重启续推**：所有状态持久化在 SQLite；重启后已确认未结束的交接、服务中和排队中的票继续存在，停机期间到期的占用在恢复时按确定顺序补齐释放。
