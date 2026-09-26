"""分流中枢的 HTTP/JSON 边界，路径统一以 /triage 开头。"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

from ..errors import DomainError, ValidationError
from .service import TriageService


def route(service: TriageService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到分流服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    query = parse_qs(parsed.query)

    def one(name: str) -> str:
        value = query.get(name, [""])[0]
        if not value:
            raise ValidationError(f"{name} 不能为空")
        return value

    try:
        if method == "POST" and parsed.path == "/triage/zones":
            receipt = service.register_zone(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/zones/capacity":
            receipt = service.set_zone_capacity(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/zones/suspend":
            receipt = service.suspend_zone(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/zones/resume":
            receipt = service.resume_zone(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/experts":
            receipt = service.register_expert(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/experts/update":
            receipt = service.update_expert(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/participants/intake":
            receipt = service.intake_participant(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/participants/review":
            receipt = service.review_participant(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/route":
            receipt = service.route_participant(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/expire-tickets":
            receipt = service.expire_tickets(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/call-next":
            receipt = service.call_next(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/check-in":
            receipt = service.check_in(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/finish-service":
            receipt = service.finish_service(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/reassign":
            receipt = service.reassign(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/handshakes/request":
            receipt = service.request_handshake(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/handshakes/confirm":
            receipt = service.confirm_handshake(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/handshakes/complete":
            receipt = service.complete_handshake(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/handshakes/cancel":
            receipt = service.cancel_handshake(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/journey/complete":
            receipt = service.complete_journey(actor_id=actor_id, **body)
        elif method == "POST" and parsed.path == "/triage/recover":
            return 200, service.recover(actor_id or "system")
        elif method == "GET" and parsed.path == "/triage/state-version":
            return 200, {"state_version": service.current_state_version(actor_id)}
        elif method == "GET" and parsed.path == "/triage/zone-pressures":
            items = service.zone_pressures(actor_id, one("site_id"))
            return 200, {"items": [item.__dict__ for item in items]}
        elif method == "GET" and parsed.path == "/triage/handshakes":
            items = service.pending_handshakes(actor_id, one("site_id"))
            return 200, {"items": [item.__dict__ for item in items]}
        elif method == "GET" and parsed.path == "/triage/participant":
            participant = service.get_participant(actor_id, one("participant_id"))
            return 200, {
                "participant_id": participant.participant_id,
                "site_id": participant.site_id,
                "status": participant.status,
                "high_risk": participant.high_risk,
                "contraindications": sorted(participant.contraindications),
                "risk_statements": sorted(participant.risk_statements),
                "preferences": list(participant.preferences),
                "accepted_services": list(participant.accepted_services),
                "current_assignment": participant.current_assignment,
                "state_version": participant.state_version,
                "updated_at": participant.updated_at,
            }
        elif method == "GET" and parsed.path == "/triage/journey":
            return 200, {"items": service.journey_events(actor_id, one("participant_id"))}
        else:
            return 404, {"error": "route_not_found", "message": "分流接口不存在"}
        return 200 if receipt.replayed else 201, receipt.to_dict()
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}
