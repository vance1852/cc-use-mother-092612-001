"""分流中枢的 SQLite 表结构与状态版本表。

所有可变业务表都携带 version 字段；state_versions 以单行单调序号记录每次
配置变更，分派结果保存所依据的版本号，供协调员事后核对"改派依据"。
"""

from __future__ import annotations

TRIAGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS triage_state_versions (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL CHECK(version >= 0)
);
CREATE TABLE IF NOT EXISTS triage_zones (
    zone_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    service_type TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity >= 1),
    serving_limit INTEGER NOT NULL CHECK(serving_limit >= 1),
    status TEXT NOT NULL,
    suspended_at TEXT,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS triage_experts (
    expert_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    display_name TEXT NOT NULL,
    qualifications_json TEXT NOT NULL,
    on_duty INTEGER NOT NULL CHECK(on_duty IN (0, 1)),
    zone_id TEXT REFERENCES triage_zones(zone_id),
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS triage_participants (
    participant_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    status TEXT NOT NULL,
    high_risk INTEGER NOT NULL CHECK(high_risk IN (0, 1)),
    contraindications_json TEXT NOT NULL,
    risk_statements_json TEXT NOT NULL,
    preferences_json TEXT NOT NULL,
    accepted_services_json TEXT NOT NULL,
    current_zone_id TEXT,
    current_ticket_id TEXT,
    state_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS triage_journey_events (
    event_id TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES triage_participants(participant_id),
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    zone_id TEXT,
    expert_id TEXT,
    ticket_id TEXT,
    state_version INTEGER,
    detail_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    UNIQUE(participant_id, seq)
);
CREATE TABLE IF NOT EXISTS triage_tickets (
    ticket_id TEXT PRIMARY KEY,
    zone_id TEXT NOT NULL REFERENCES triage_zones(zone_id),
    participant_id TEXT NOT NULL REFERENCES triage_participants(participant_id),
    state TEXT NOT NULL,
    seq_in_zone INTEGER NOT NULL,
    miss_seq INTEGER,
    miss_count INTEGER NOT NULL DEFAULT 0,
    issued_at TEXT NOT NULL,
    called_at TEXT,
    expires_at TEXT,
    check_in_at TEXT,
    released_at TEXT,
    released_reason TEXT,
    finished_at TEXT,
    UNIQUE(zone_id, seq_in_zone)
);
CREATE INDEX IF NOT EXISTS idx_triage_tickets_zone_state ON triage_tickets(zone_id, state);
CREATE TABLE IF NOT EXISTS triage_handshakes (
    handshake_id TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES triage_participants(participant_id),
    ticket_id TEXT NOT NULL REFERENCES triage_tickets(ticket_id),
    from_zone_id TEXT NOT NULL REFERENCES triage_zones(zone_id),
    to_zone_id TEXT NOT NULL REFERENCES triage_zones(zone_id),
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    confirmed_by TEXT,
    confirmed_at TEXT,
    state_version INTEGER NOT NULL,
    new_ticket_id TEXT REFERENCES triage_tickets(ticket_id),
    completed_at TEXT,
    CHECK(from_zone_id <> to_zone_id)
);
CREATE INDEX IF NOT EXISTS idx_triage_handshakes_status ON triage_handshakes(status);
"""


def ensure_triage_schema(connection) -> None:
    """幂等建表并初始化状态版本序号。"""

    connection.executescript(TRIAGE_SCHEMA)
    connection.execute(
        "INSERT INTO triage_state_versions(singleton, version) VALUES (1, 0) "
        "ON CONFLICT(singleton) DO NOTHING"
    )
