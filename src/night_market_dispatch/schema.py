"""定义分流中枢在基础层之上新增的表结构。"""

from __future__ import annotations

DISPATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS dispatch_zones (
    zone_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    service_type TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity >= 0),
    status TEXT NOT NULL CHECK(status IN ('open', 'paused', 'closed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_experts (
    expert_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    qualifications_json TEXT NOT NULL,
    zone_id TEXT REFERENCES dispatch_zones(zone_id),
    status TEXT NOT NULL CHECK(status IN ('on_duty', 'off_duty')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_participants (
    participant_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    alias TEXT NOT NULL,
    requests_json TEXT NOT NULL,
    contraindications_json TEXT NOT NULL,
    risk_flags_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('manual_review', 'waiting', 'called', 'serving', 'paused', 'done')),
    current_zone_id TEXT REFERENCES dispatch_zones(zone_id),
    queue_number INTEGER,
    called_at TEXT,
    call_expires_at TEXT,
    completed_json TEXT NOT NULL,
    screened INTEGER NOT NULL CHECK(screened IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dispatch_participants_zone
    ON dispatch_participants(current_zone_id, status);
CREATE INDEX IF NOT EXISTS idx_dispatch_participants_site
    ON dispatch_participants(site_id, status);
CREATE TABLE IF NOT EXISTS dispatch_assignments (
    assignment_id TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES dispatch_participants(participant_id),
    zone_id TEXT NOT NULL REFERENCES dispatch_zones(zone_id),
    queue_number INTEGER,
    based_on_version INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dispatch_assignments_zone
    ON dispatch_assignments(zone_id, queue_number);
CREATE TABLE IF NOT EXISTS dispatch_itinerary (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    participant_id TEXT NOT NULL REFERENCES dispatch_participants(participant_id),
    event_type TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    zone_id TEXT,
    detail_json TEXT NOT NULL,
    based_on_version INTEGER,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dispatch_itinerary_participant
    ON dispatch_itinerary(participant_id, seq);
CREATE TABLE IF NOT EXISTS dispatch_handovers (
    handover_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    participant_id TEXT NOT NULL REFERENCES dispatch_participants(participant_id),
    from_zone_id TEXT NOT NULL REFERENCES dispatch_zones(zone_id),
    to_zone_id TEXT NOT NULL REFERENCES dispatch_zones(zone_id),
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'completed', 'cancelled')),
    initiated_by TEXT NOT NULL,
    resolved_by TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispatch_handovers_site
    ON dispatch_handovers(site_id, status);
CREATE TABLE IF NOT EXISTS dispatch_state_versions (
    site_id TEXT PRIMARY KEY REFERENCES sites(site_id),
    version INTEGER NOT NULL CHECK(version >= 0)
);
"""


def ensure_schema(connection) -> None:
    """在既有基础层数据库上补齐分流中枢表结构。"""

    connection.executescript(DISPATCH_SCHEMA)
