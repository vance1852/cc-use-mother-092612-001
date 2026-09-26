"""定义分流中枢使用的结构化参考码与规则常量。

系统只依据登记在案的结构化编码做规则匹配，不生成任何诊断结论：
- 高风险陈述一律转人工复核；
- 禁忌与服务类型的不兼容关系仅用于回避不适合的体验项目。
"""

from __future__ import annotations

# 允许登记的服务类型
SERVICE_TYPES = frozenset({
    "consultation",   # 义诊
    "massage",        # 推拿体验
    "culture_talk",   # 文化讲解
})

# 被视为高风险、必须转人工的陈述编码
HIGH_RISK_STATEMENTS = frozenset({
    "chest_pain",            # 胸痛胸闷
    "dyspnea",               # 呼吸困难
    "syncope",               # 晕厥史
    "acute_injury",          # 急性外伤
    "fever_acute",           # 急性发热
    "severe_pain",           # 剧烈疼痛
    "pregnancy",             # 妊娠
    "hypertensive_crisis",   # 血压危象
})

# 禁忌编码 -> 与该禁忌不兼容的服务类型
CONTRAINDICATION_MAP = {
    "skin_lesion": frozenset({"massage"}),
    "acute_sprain": frozenset({"massage"}),
    "osteoporosis_severe": frozenset({"massage"}),
    "pregnancy": frozenset({"massage"}),
    "cardiac_instability": frozenset({"massage", "consultation"}),
}

# 区域运行状态
ZONE_STATUSES = frozenset({"active", "suspended", "closed"})

# 行程参与者状态
PARTICIPANT_STATUSES = frozenset({
    "screened",       # 已完成风险筛查、可分流
    "manual_review",  # 转人工处理
    "routed",         # 已分派（排队/候诊/服务中由叫号票表达）
    "completed",      # 行程结束
})

# 叫号票状态
TICKET_STATES = frozenset({"waiting", "called", "serving", "released", "finished"})

# 交接状态
HANDSHAKE_STATUSES = frozenset({"pending", "confirmed", "cancelled", "completed"})

# 默认叫号超时（秒）
DEFAULT_CALL_TIMEOUT_SECONDS = 300
# 暂停状态下叫号占用的宽限：暂停后占用最迟在该宽限结束时释放（早于正常超时），
# 避免暂停区域长期挂住群众；恢复后过号票按 miss_seq 优先重呼。
SUSPEND_GRACE_SECONDS = 120
# 重呼过号群众时允许的最大过号次数，超过则需重新入场分流
MAX_MISS_COUNT = 2


def incompatible_services(contraindications: frozenset[str]) -> frozenset[str]:
    """根据禁忌编码集合返回应回避的服务类型集合。"""

    blocked: set[str] = set()
    for code in contraindications:
        blocked.update(CONTRAINDICATION_MAP.get(code, frozenset()))
    return frozenset(blocked)


def is_high_risk(statement_codes) -> bool:
    """只要陈述中包含任一高风险编码，就必须转人工。"""

    return any(code in HIGH_RISK_STATEMENTS for code in (statement_codes or ()))
