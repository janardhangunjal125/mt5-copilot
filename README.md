# MT5 <-> Telegram Command Relay

Generic plumbing for remote-controlling a MetaTrader 5 terminal from
Telegram (or any bot/operator), plus event reporting back. Two halves:

- **`CopilotEA.mq5`** — an MT5 Expert Advisor that reports account
  events (positions, deals, candles, heartbeat) to a watcher URL, and —
  only if you deliberately enable it — polls the watcher for operator
  commands and executes them: `ENTER`, `SET_SLTP`, `CLOSE`, `CLOSE_ALL`.
- **`watcher_server.py`** — a small Python HTTP server that receives
  the EA's events, forwards selected ones to Telegram, and hosts the
  command channel the EA polls. It also accepts TradingView alert
  webhooks (`POST /tv/<secret>`) and turns them into `ENTER` commands.

This is transport only. There is **no trading strategy here**: no
signals, no indicators, no breakeven logic, no session locks, no
automatic stop/target management. What goes in is what you send; the
EA executes explicit commands and acks each one.

## Architecture

```
Telegram bot/operator
      |  creates commands (POST /commands)
      v
watcher_server.py  ---- Telegram alerts (events, command results, ticker)
      |  ^  polls (GET /commands/pending), acks (POST /commands/ack)
      |  |  reports (POST /report)
      v  |
CopilotEA.mq5  on your MT5 terminal
      |
TradingView (optional) -- alert webhook --> POST /tv/<secret>
```

Command flow: `POST /commands` → stored `pending` with a TTL (default
20s) → EA's `GET /commands/pending` delivers each command **exactly
once** (delivered commands are never re-served, so an un-acked ENTER
can't be double-executed) → EA prints the full command to the Experts
log, executes, and `POST /commands/ack` with `executed` / `rejected` /
`failed` + detail. Created / delivered / expired / result steps are all
appended to the per-day `events/*.jsonl` audit log.

## What the EA reports

| Event | When |
|---|---|
| `POSITION_OPEN` | A position appears |
| `POSITION_SLTP_CHANGE` | Stop-loss or take-profit changes |
| `POSITION_CLOSE` | A position disappears |
| `DEAL` | Every entry/exit fill |
| `CANDLE` | Last 3 closed candles, on each new bar |
| `HEARTBEAT` | Every 60s (configurable): balance, equity, open count |

## Safety, stated plainly

- **Demo lock:** the EA refuses to start on anything but a demo
  account — `OnInit` checks the account trade mode and fails
  otherwise, and every command is re-checked before execution.
- **Master switch:** commands are rejected unless
  `InpAllowExecution=true` is set when you attach the EA.
- **Volume cap:** an `ENTER` above `InpMaxVolume` is rejected.
- **Audit:** every command is printed to the Experts log *before* it
  runs; the outcome is acked back after it runs; the watcher keeps a
  JSONL audit trail of every lifecycle step.
- The EA never sees or sends your account password.

That said: this tool *can* place and close real orders on the account
it's attached to. The demo lock is a seatbelt, not a guarantee — read
the source before you run it, keep the demo lock on, and don't expose
the watcher to the open internet (run it on localhost behind a tunnel
or firewall you control).

## Setup

### 1. The watcher (your server)

Python 3.10+, stdlib only (no pip packages).

```bash
export MT5_COPILOT_TOKEN=<a long random shared secret>
export TELEGRAM_BOT_TOKEN=<bot token from @BotFather>      # optional
export TELEGRAM_CHAT_ID=<your chat id>                     # optional
export TV_WEBHOOK_SECRET=<a long random secret>            # optional, TradingView only
export WATCHER_DISPLAY_TZ=<IANA timezone>                  # optional, default Europe/Berlin
python3 watcher_server.py [port]                           # default port 8734
```

The server binds to `127.0.0.1` and **refuses to start** if
`MT5_COPILOT_TOKEN` is unset or still the `YOUR_...` placeholder. It
also refuses to serve anything without the token. You can edit the
`YOUR_..._HERE` placeholders at the top of `watcher_server.py` instead
of env vars, but env vars are the cleaner route.

Optional `WATCHER_COMMAND_TTL_SECONDS` overrides the 20s command TTL
(useful in testing; a slow-polling EA needs a longer TTL than the poll
interval).

### 2. The EA (your MT5 terminal)

1. In MT5: **File → Open Data Folder → MQL5 → Experts**, copy
   `CopilotEA.mq5` there (see `CopilotEA_inputs.set` for a sample).
2. Open it in MetaEditor and compile (**F7**) — expect "0 errors".
3. Drag the EA onto **any one chart** of a **demo** account.
4. **Tools → Options → Expert Advisors → "Allow WebRequest for listed
   URL"** and add your watcher base URL.
5. Set `InpWatcherURL` (e.g. `https://<your-tunnel>/report`) and
   `InpSharedSecret` (same value as `MT5_COPILOT_TOKEN`). Set
   `InpAllowExecution=true` only if you want the EA to execute
   commands — with it `false`, it reports only and rejects commands.

By default (empty `InpWatcherURL`) the EA runs in LOCAL MODE: events
go to a log file in the MT5 Files folder and the Experts journal, and
command polling is off. Nothing leaves your computer.

## Command API

All protected by the `X-Reporter-Token: <MT5_COPILOT_TOKEN>` header.

```bash
# create a command
curl -X POST localhost:8734/commands -H "X-Reporter-Token: $T" \
  -H 'Content-Type: application/json' \
  -d '{"type":"ENTER","symbol":"US100","side":"BUY","volume":0.01,"sl":19000,"tp":19200}'

# the EA polls this; each command is served exactly once
curl localhost:8734/commands/pending -H "X-Reporter-Token: $T"

# ack (sent by the EA; shown here for completeness)
curl -X POST localhost:8734/commands/ack -H "X-Reporter-Token: $T" \
  -H 'Content-Type: application/json' \
  -d '{"id":"<cmd-id>","status":"executed","detail":"filled price=19100.5"}'
```

Command types: `ENTER` (market order with optional SL/TP),
`SET_SLTP` (move a position's stop/target by `position_id`; a level
absent from the command keeps its current value), `CLOSE`
(`position_id`), `CLOSE_ALL`.

TradingView bridge: set a TradingView alert webhook URL to
`https://<your-server>/tv/<TV_WEBHOOK_SECRET>` with a JSON body like
`{"action":"BUY","symbol":"US100","volume":0.01,"sl":19000,"tp":19200}`.
Symbols are translated through the `TV_SYMBOL_MAP` dict at the top of
`watcher_server.py` — edit it to match your broker's symbol names. A
webhook with `sl`/`tp` omitted places the order **without** SL/TP;
nothing in this repo manages the trade afterwards.

## Telegram

If `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set, the watcher
pushes: entries, SL/TP changes, closes, and a once-a-minute position
ticker (price estimate, distance to stop/target, equity). Alerts are
fire-and-forget via curl and never block event ingestion.

## Honest notes

- **Trade-management rules are intentionally not included.** This repo
  ships the relay plumbing only. Anything that decides *when* to move
  a stop, when to lock a session, or how to size a position is your
  own strategy — build it on top of the command API, or run it as a
  separate layer.
- The `events/*.jsonl` log and `position_state.json` contain your
  account numbers, balance, and trade history — they're gitignored,
  keep them that way.
- The in-memory command store dies with the process; commands pending
  at restart are lost (expired, never delivered). For a throwaway
  relay this is fine; for anything serious, add persistence.
- The EA streams candles on `OnTick` and polls commands on a 1-second
  timer — fine for a handful of charts, wasteful at scale.

## License

MIT — see [LICENSE](LICENSE). Use it, fork it, ship it; no warranty.
Trading involves risk; this tool executes what you tell it to, it does
not advise.
