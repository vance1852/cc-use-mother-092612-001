"""分流中枢的 HTTP/JSON 边界，未匹配的路径回退到基础层路由。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from night_market_foundation.api import route as foundation_route
from night_market_foundation.errors import DomainError, ValidationError
from night_market_foundation.service import DomainService
from night_market_foundation.storage import Database

from .service import DispatchService


def route(dispatch: DispatchService, foundation: DomainService, method: str, path: str,
          body: dict[str, Any] | None, headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到分流中枢或基础层服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if method == "POST" and parsed.path == "/dispatch/zones":
            return _decision(dispatch.register_zone(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/zones/capacity":
            return _decision(dispatch.update_zone_capacity(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/zones/status":
            return _decision(dispatch.set_zone_status(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/experts":
            return _decision(dispatch.register_expert(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/experts/assign":
            return _decision(dispatch.assign_expert(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/experts/status":
            return _decision(dispatch.set_expert_status(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/check-in":
            return _decision(dispatch.check_in_participant(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/call-next":
            return _decision(dispatch.call_next(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/arrive":
            return _decision(dispatch.confirm_arrival(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/complete":
            return _decision(dispatch.complete_service(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/pause":
            return _decision(dispatch.pause_participant(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/resume":
            return _decision(dispatch.resume_participant(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/settle-timeouts":
            return _decision(dispatch.settle_timeouts(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/recompute":
            return _decision(dispatch.recompute_assignments(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/manual-review/resolve":
            return _decision(dispatch.resolve_manual_review(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/handovers/initiate":
            return _decision(dispatch.initiate_handover(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/handovers/complete":
            return _decision(dispatch.complete_handover(actor_id=actor_id, **body))
        if method == "POST" and parsed.path == "/dispatch/handovers/cancel":
            return _decision(dispatch.cancel_handover(actor_id=actor_id, **body))
        if method == "GET" and parsed.path == "/dispatch/pressure":
            site_id = parse_qs(parsed.query).get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, dispatch.get_zone_pressure(site_id)
        if method == "GET" and parsed.path == "/dispatch/participant":
            participant_id = parse_qs(parsed.query).get("participant_id", [""])[0]
            if not participant_id:
                raise ValidationError("participant_id 不能为空")
            return 200, dispatch.get_participant(participant_id)
        if method == "GET" and parsed.path == "/dispatch/itinerary":
            participant_id = parse_qs(parsed.query).get("participant_id", [""])[0]
            if not participant_id:
                raise ValidationError("participant_id 不能为空")
            return 200, {"items": dispatch.get_itinerary(participant_id)}
        if method == "GET" and parsed.path == "/dispatch/handovers/pending":
            site_id = parse_qs(parsed.query).get("site_id", [""])[0]
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, {"items": dispatch.list_pending_handovers(site_id)}
        return foundation_route(foundation, method, path, body, headers)
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


def _decision(result: tuple[dict[str, Any], bool]) -> tuple[int, dict[str, Any]]:
    decision, replayed = result
    return (200 if replayed else 201), {**decision, "replayed": replayed}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    dispatch: DispatchService
    foundation: DomainService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.dispatch, self.foundation, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动分流中枢 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动中医文化夜市服务分流中枢")
    parser.add_argument("--database", default="dispatch.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.foundation = DomainService(database)
    Handler.dispatch = DispatchService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
