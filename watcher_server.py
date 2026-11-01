#!/usr/bin/env python3
"""
MT5 <-> Telegram command relay — watcher/bridge server.

The plumbing half of the relay. The CopilotEA (an MT5 Expert Advisor)
POSTs JSON trade events here, and polls here for operator commands.

Event side: receives JSON events POSTed by the MT5 EA, checks a shared
secret token, and appends each event to a per-day JSONL file under
events/. Also serves a tiny token-protected /status endpoint, and
forwards selected events to Telegram.

Command channel: an operator creates a command, the EA polls for it,
executes it on the DEMO account, and acks the result.
    POST /commands          create {type, symbol, side, volume, sl, tp,
                            position_id}; type is one of ENTER,
                            SET_SLTP, CLOSE, CLOSE_ALL (case-insensitive
                            in, canonical case out). The command gets an
                            id, created_at, expires_at (created + TTL,
                            default 20s) and status "pending".
    GET  /commands/pending  pending, unexpired commands. Expired ones
                            are marked expired and never delivered.
    POST /commands/ack      {id, status: executed|rejected|failed,
                            detail} — records the outcome.
Every step (created / delivered / expired / result) is appended to the
same events JSONL log as the audit trail.

TradingView bridge (optional): POST /tv/<secret> accepts a TradingView
alert webhook and translates it into an ENTER command. TradingView
can't send custom headers, so the secret lives in the URL path.

Run:
    export MT5_COPILOT_TOKEN=<shared secret>
    export TELEGRAM_BOT_TOKEN=<bot token> TELEGRAM_CHAT_ID=<chat id>
    python3 watcher_server.py [port]

Secrets are read from the environment ONLY — never hard-code them,
never commit them, never paste them in chat.
Optional: WATCHER_COMMAND_TTL_SECONDS overrides the 20s command TTL
(testing only).
"""
import json
import os
import sys
import hmac
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

BASE = Path(__file__).resolve().parent
EVENTS_DIR = BASE / "events"

# --- Setup: replace the placeholders below (or set the matching env
# vars) before running. Anything starting with "YOUR_" is treated as
# "not configured" and is never used as a real credential.
TOKEN = os.environ.get("MT5_COPILOT_TOKEN", "YOUR_SHARED_SECRET_HERE")
COMMAND_TTL = float(os.environ.get("WATCHER_COMMAND_TTL_SECONDS", "20"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")
TV_WEBHOOK_SECRET = os.environ.get("TV_WEBHOOK_SECRET", "YOUR_TV_WEBHOOK_SECRET_HERE")
# Timezone used for the once-a-minute Telegram ticker display.
DISPLAY_TZ = os.environ.get("WATCHER_DISPLAY_TZ", "Europe/Berlin")


def _is_placeholder(value: str) -> bool:
    return not value or value.startswith("YOUR_")


# TradingView -> MT5 bridge. TV alert webhooks land here (secret in the URL
# path — TV can't send custom headers) and are translated into EA commands.
# EXAMPLE symbol map — edit to match YOUR broker's symbol names.
TV_SYMBOL_MAP = {
    "US100": "US100",
    "XAUUSD": "XAUUSD",
    "GOLD": "XAUUSD",
}
TV_MAX_VOLUME = 0.10


def telegram_send(text: str) -> None:
    """Best-effort Telegram alert (fire-and-forget; never blocks events).
    Uses curl rather than urllib — it proved more reliable behind
    restrictive egress proxies."""
    if _is_placeholder(TELEGRAM_BOT_TOKEN) or _is_placeholder(TELEGRAM_CHAT_ID):
        return

    def _send():
        try:
            import subprocess

            payload = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text})
            r = subprocess.run(
                ["curl", "-s", "-m", "15", "-X", "POST",
                 f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                 "-H", "Content-Type: application/json",
                 "-d", payload],
                capture_output=True, timeout=20)
            if r.returncode != 0:
                print(f"[telegram] curl rc={r.returncode}: {r.stderr[:120]!r}",
                      flush=True)
        except Exception as exc:  # alerting must never break the watcher
            print(f"[telegram] send failed: {exc}", flush=True)

    threading.Thread(target=_send, daemon=True).start()


def telegram_for_event(payload: dict) -> None:
    ev = payload.get("event")
    if ev == "POSITION_OPEN":
        price = payload.get("price") or payload.get("open_price")
        line = (
            f"📈 ENTRY — {payload.get('side', '?')} {payload.get('symbol', '?')} "
            f"{payload.get('volume', '?')} lots"
        )
        if price:
            line += f" @ {price}"
        telegram_send(line)
    elif ev == "POSITION_SLTP_CHANGE":
        telegram_send(
            f"🎯 SL/TP set — SL {payload.get('sl')} · TP {payload.get('tp')} "
            f"(profit {payload.get('profit')})"
        )
    elif ev == "POSITION_CLOSE":
        telegram_send(
            f"🏁 CLOSED — {payload.get('symbol', '')} "
            f"profit {payload.get('profit')} · balance {payload.get('balance')}"
        )
    elif ev == "UNPROTECTED":
        telegram_send(f"🔔 {payload.get('text', 'a position is missing SL/TP')}")


# ------------------- MINUTE TICKER (Telegram, script-side) -------------------
# The watcher itself composes the once-a-minute position update and pushes
# it to Telegram — no agent in the loop, so it lands within seconds.
# Position state persists to a small JSON file so a watcher restart
# mid-trade does not lose the open position.
STATE_FILE = BASE / "position_state.json"
_state_lock = threading.Lock()
_state = {
    "position": None,
    "balance": None,
    "equity": None,
    "last_event_at": None,
    "last_tick_at": 0.0,
    "quiet_sent": False,
}


def _save_state() -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_state["position"]), encoding="utf-8")
        tmp.replace(STATE_FILE)
    except Exception:  # noqa: BLE001
        pass


def _load_state() -> None:
    try:
        if STATE_FILE.exists():
            _state["position"] = json.loads(
                STATE_FILE.read_text(encoding="utf-8")
            )
    except Exception:  # noqa: BLE001
        _state["position"] = None


def track_state(payload: dict) -> None:
    ev = payload.get("event")
    if ev not in (
        "POSITION_OPEN",
        "POSITION_SLTP_CHANGE",
        "POSITION_CLOSE",
        "HEARTBEAT",
    ):
        return
    with _state_lock:
        _state["last_event_at"] = time.time()
        _state["quiet_sent"] = False
        if payload.get("balance") is not None:
            _state["balance"] = payload["balance"]
        if payload.get("equity") is not None:
            _state["equity"] = payload["equity"]
        if ev in ("POSITION_OPEN", "POSITION_SLTP_CHANGE"):
            if _state["position"] is None or ev == "POSITION_OPEN":
                _state["position"] = {
                    k: payload.get(k)
                    for k in (
                        "position_id",
                        "symbol",
                        "side",
                        "volume",
                        "price_open",
                        "sl",
                        "tp",
                    )
                }
            else:
                _state["position"]["sl"] = payload.get("sl")
                _state["position"]["tp"] = payload.get("tp")
            _save_state()
        elif ev == "POSITION_CLOSE":
            _state["position"] = None
            _save_state()


def _ticker_loop() -> None:
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(DISPLAY_TZ)
    while True:
        time.sleep(15)
        try:
            with _state_lock:
                pos = dict(_state["position"]) if _state["position"] else None
                bal, eq = _state["balance"], _state["equity"]
                last_ev = _state["last_event_at"]
                last_tick = _state["last_tick_at"]
                quiet = _state["quiet_sent"]
            if not pos or bal is None or eq is None or not last_ev:
                continue
            if time.time() - last_ev > 180:
                if not quiet:
                    telegram_send(
                        "📴 No fresh reports from MT5 for 3+ minutes — "
                        "the position rides on its broker-side stop/target."
                    )
                    with _state_lock:
                        _state["quiet_sent"] = True
                continue
            if time.time() - last_tick < 55:
                continue
            floating = eq - bal
            entry = pos.get("price_open")
            if entry is None:
                continue
            price = entry + floating if pos.get("side") == "BUY" else entry - floating
            text = (
                f"⏱️ {datetime.now(tz).strftime('%H:%M')} · "
                f"{'LONG' if pos.get('side') == 'BUY' else 'SHORT'} "
                f"{pos.get('symbol')} {pos.get('volume')} @ {entry} · "
                f"now ~{price:.1f} ({floating:+.1f})"
            )
            if pos.get("sl"):
                text += f" · stop {abs(price - pos['sl']):.0f} pts away"
            if pos.get("tp"):
                text += f" · target {abs(pos['tp'] - price):.0f} pts away"
            text += f" · equity {eq}"
            if floating < 0:
                text = "🔴 " + text
            telegram_send(text)
            with _state_lock:
                _state["last_tick_at"] = time.time()
            print(f"[ticker] sent {text}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[ticker] error: {exc}", flush=True)


def event_file():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return EVENTS_DIR / f"{day}.jsonl"


def append_event(payload: dict) -> None:
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    payload["_received_at"] = datetime.now(timezone.utc).isoformat()
    with event_file().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    telegram_for_event(payload)
    track_state(payload)


def status() -> dict:
    f = event_file()
    if not f.exists():
        return {"events_today": 0, "last_event": None}
    lines = f.read_text(encoding="utf-8").strip().splitlines()
    last = None
    if lines:
        try:
            last = json.loads(lines[-1])
        except json.JSONDecodeError:
            last = None
    return {"events_today": len(lines), "last_event": last}


# ------------------------- COMMAND CHANNEL -------------------------
# In-memory store: id -> record. Records keep private datetime fields
# (prefixed "_") for expiry maths; the public view strips them.
COMMANDS: dict = {}
COMMANDS_LOCK = threading.Lock()
COMMAND_TYPES = {
    "ENTER": "ENTER",
    "SET_SLTP": "SET_SLTP",
    "CLOSE": "CLOSE",
    "CLOSE_ALL": "CLOSE_ALL",
}
ACK_STATUSES = ("executed", "rejected", "failed")


def _public_command(rec: dict) -> dict:
    return {
        "id": rec["id"],
        "type": rec["type"],
        "symbol": rec.get("symbol"),
        "side": rec.get("side"),
        "volume": rec.get("volume"),
        "sl": rec.get("sl"),
        "tp": rec.get("tp"),
        "position_id": rec.get("position_id"),
        "status": rec["status"],
        "created_at": rec["created_at"],
        "expires_at": rec["expires_at"],
    }


DELIVERED_ACK_TIMEOUT = 90.0  # seconds a delivered command may await its ack


def _sweep_expired(now: datetime) -> None:
    """Mark stale commands as expired (audit-logged). Pending commands die
    at their TTL; delivered-but-unacked commands die after the ack timeout
    (the EA acks within seconds — silence means it never executed).
    Caller must hold COMMANDS_LOCK."""
    for rec in COMMANDS.values():
        stale = (
            (rec["status"] == "pending" and now >= rec["_expires_dt"])
            or (rec["status"] == "delivered"
                and (now - rec["_delivered_dt"]).total_seconds() > DELIVERED_ACK_TIMEOUT)
        )
        if stale:
            rec["status"] = "expired"
            append_event({
                "event": "COMMAND_EXPIRED",
                "command_id": rec["id"],
                "type": rec["type"],
            })


def create_command(payload: dict) -> tuple:
    """Validate + store a command. Returns (http_code, response_dict)."""
    raw_type = str(payload.get("type", "")).strip().upper()
    if raw_type not in COMMAND_TYPES:
        return 400, {"ok": False, "error": "type must be one of ENTER, SET_SLTP, CLOSE, CLOSE_ALL"}
    now = datetime.now(timezone.utc)
    expires = now + timedelta(seconds=COMMAND_TTL)
    rec = {
        "id": uuid.uuid4().hex,
        "type": COMMAND_TYPES[raw_type],
        "symbol": payload.get("symbol"),
        "side": payload.get("side"),
        "volume": payload.get("volume"),
        "sl": payload.get("sl"),
        "tp": payload.get("tp"),
        "position_id": payload.get("position_id"),
        "status": "pending",
        "created_at": now.isoformat(),
        "expires_at": expires.isoformat(),
        "_created_dt": now,
        "_expires_dt": expires,
        "_delivered": False,
        "ack": None,
    }
    with COMMANDS_LOCK:
        COMMANDS[rec["id"]] = rec
        append_event({"event": "COMMAND_CREATED", "command": _public_command(rec)})
    return 201, {"ok": True, "command": _public_command(rec)}


def pending_commands() -> list:
    """Serve each command EXACTLY ONCE. A served command becomes
    "delivered" and is never served again — otherwise an un-acked ENTER
    would be re-executed on every 1s poll. Delivery is audit-logged."""
    now = datetime.now(timezone.utc)
    out = []
    with COMMANDS_LOCK:
        _sweep_expired(now)
        for rec in COMMANDS.values():
            if rec["status"] != "pending":
                continue
            rec["status"] = "delivered"
            rec["_delivered_dt"] = now
            append_event({
                "event": "COMMAND_DELIVERED",
                "command_id": rec["id"],
                "type": rec["type"],
            })
            out.append(_public_command(rec))
    return out


def ack_command(payload: dict) -> tuple:
    """Record the EA's outcome for a command. Returns (http_code, dict)."""
    cmd_id = payload.get("id")
    ack_status = payload.get("status")
    detail = payload.get("detail", "")
    if not cmd_id or ack_status not in ACK_STATUSES:
        return 400, {"ok": False, "error": "ack needs id and status in executed|rejected|failed"}
    with COMMANDS_LOCK:
        rec = COMMANDS.get(cmd_id)
        if rec is None:
            return 404, {"ok": False, "error": "unknown command id"}
        if rec["status"] in ACK_STATUSES:
            return 409, {"ok": False, "error": "command already finished"}
        rec["status"] = ack_status
        rec["ack"] = {"status": ack_status, "detail": detail,
                      "acked_at": datetime.now(timezone.utc).isoformat()}
        append_event({
            "event": "COMMAND_RESULT",
            "command_id": rec["id"],
            "type": rec["type"],
            "status": ack_status,
            "detail": detail,
        })
    return 200, {"ok": True, "command": _public_command(rec)}


def handle_tv_webhook(payload: dict) -> tuple:
    """Translate a TradingView alert into an EA ENTER command.
    Expected body: {"action": "BUY"|"SELL", "symbol": "<your symbol>",
                    "volume": 0.01 (optional), "sl": <price> (optional),
                    "tp": <price> (optional)}.
    The symbol must be a key of TV_SYMBOL_MAP. Missing sl/tp -> 0/0 ->
    the EA places the order without SL/TP; anything beyond that (e.g.
    automatic SL/TP management) is your own strategy, not this relay."""
    action = str(payload.get("action", "")).strip().upper()
    if action not in ("BUY", "SELL"):
        return 400, {"ok": False, "error": "action must be BUY or SELL"}
    tv_symbol = str(payload.get("symbol", "")).strip().upper()
    symbol = TV_SYMBOL_MAP.get(tv_symbol)
    if not symbol:
        return 400, {"ok": False,
                     "error": "symbol must be one of " + ", ".join(sorted(TV_SYMBOL_MAP))}
    try:
        volume = float(payload.get("volume", 0.01))
    except (TypeError, ValueError):
        return 400, {"ok": False, "error": "volume must be a number"}
    if not (0 < volume <= TV_MAX_VOLUME):
        return 400, {"ok": False, "error": f"volume must be within 0 < v <= {TV_MAX_VOLUME:g}"}
    try:
        sl = float(payload.get("sl", 0) or 0)
        tp = float(payload.get("tp", 0) or 0)
    except (TypeError, ValueError):
        return 400, {"ok": False, "error": "sl/tp must be prices"}
    if sl < 0 or tp < 0:
        return 400, {"ok": False, "error": "sl/tp must be >= 0"}
    code, resp = create_command({
        "type": "ENTER",
        "symbol": symbol,
        "side": action,
        "volume": volume,
        "sl": sl,
        "tp": tp,
    })
    if code == 201:
        cmd_id = resp["command"]["id"]
        append_event({"event": "TV_WEBHOOK_RECEIVED", "command_id": cmd_id,
                      "action": action, "symbol": symbol, "volume": volume})
        telegram_send(f"🌐 TV webhook → {action} {symbol} {volume:g} lots (cmd {cmd_id[:8]})")
    return code, resp


class Handler(BaseHTTPRequestHandler):
    def _token_ok(self) -> bool:
        if _is_placeholder(TOKEN):
            return False  # refuse to run open: no token configured
        return self.headers.get("X-Reporter-Token", "") == TOKEN

    def _reply(self, code: int, obj: dict):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        """Parse the request body as a JSON object; None on any failure."""
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else None
        except Exception:  # noqa: BLE001
            return None

    def do_POST(self):
        path = self.path.rstrip("/")
        if path.startswith("/tv/"):
            secret = path[len("/tv/"):]
            if _is_placeholder(TV_WEBHOOK_SECRET) or not hmac.compare_digest(secret, TV_WEBHOOK_SECRET):
                return self._reply(404, {"ok": False, "error": "unknown path"})
            payload = self._read_json_body()
            if payload is None:
                return self._reply(400, {"ok": False, "error": "body must be a JSON object"})
            code, resp = handle_tv_webhook(payload)
            return self._reply(code, resp)
        if path in ("/commands", "/commands/ack"):
            if not self._token_ok():
                return self._reply(401, {"ok": False, "error": "bad token"})
            payload = self._read_json_body()
            if payload is None:
                return self._reply(400, {"ok": False, "error": "body must be a JSON object"})
            code, resp = create_command(payload) if path == "/commands" else ack_command(payload)
            return self._reply(code, resp)
        if path != "/report":
            return self._reply(404, {"ok": False, "error": "unknown path"})
        if not self._token_ok():
            return self._reply(401, {"ok": False, "error": "bad token"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, dict) or "event" not in payload:
                return self._reply(400, {"ok": False, "error": "payload must be a JSON object with an 'event' field"})
        except Exception as exc:  # noqa: BLE001
            return self._reply(400, {"ok": False, "error": f"bad body: {exc}"})
        append_event(payload)
        return self._reply(200, {"ok": True})

    def do_GET(self):
        path = self.path.rstrip("/")
        if path == "/commands/pending":
            if not self._token_ok():
                return self._reply(401, {"ok": False, "error": "bad token"})
            return self._reply(200, {"ok": True, "commands": pending_commands()})
        if path != "/status":
            return self._reply(404, {"ok": False, "error": "unknown path"})
        if not self._token_ok():
            return self._reply(401, {"ok": False, "error": "bad token"})
        return self._reply(200, status())

    def log_message(self, fmt, *args):  # keep stdout quiet-ish
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    if _is_placeholder(TOKEN):
        sys.exit("MT5_COPILOT_TOKEN is not set (or still the placeholder) — "
                 "refusing to start an open endpoint.")
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8734
    _load_state()
    threading.Thread(target=_ticker_loop, daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"MT5<->Telegram relay watcher listening on 127.0.0.1:{port} "
          f"(POST /report, GET /status, POST /commands, "
          f"GET /commands/pending, POST /commands/ack, POST /tv/<secret>; "
          f"command TTL {COMMAND_TTL:g}s)")
    server.serve_forever()
