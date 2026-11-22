# feishu-bridge — Talk to Muse himself from Feishu (Lark)

## What this skill does

Connects Feishu (Lark) to **Muse himself** — the user's own personal Muse —
so the user can chat with Muse inside a Feishu group or a private (p2p)
chat with the bot. There is **no third-party LLM** anywhere in the chain:
replies are written by Muse, with his memory and tools.

Muse has no native Feishu connection, so this skill builds a small local
bridge out of three parts:

```
Feishu group/DM --(@bot in group; DM direct)--> feishubridge daemon
  (Go, Feishu long-connection WebSocket — NO public IP / callback URL needed)
  --> bridge-data/inbox/<message_id>.json   (+ a "Muse 正在思考…" placeholder
                                             is posted in the chat)
  --> a Muse hook polls the inbox every 10s and wakes a Muse worker
  --> Muse reads the message + chat history, writes the reply (and any
      attachments) to bridge-data/outbox/<message_id>.json
  --> the daemon updates the placeholder with the full reply and sends
      attachments (image / video / file) as follow-up replies
```

Expected behaviour: replies take tens of seconds (poll + thinking time),
arrive as one whole message (the placeholder is edited into the answer),
not token-by-token streaming.

## Privacy

This package contains **no credentials, no chat history, no personal data**.
The deploying Muse must obtain the user's own Feishu App ID / App Secret
and write them only into the local `config/bridge.json` (chmod 600). Never
copy secrets into the skill, memory files, or chat.

## Layout (after install, under `~/workspace/feishu-bridge/`)

- `bridge/` — Go source of the daemon (`main.go`, `go.mod`)
- `bin/feishubridge` — compiled daemon
- `config/bridge.json` — `{lark_app_id, lark_app_secret, bot_name}` (from
  `assets/config/bridge.example.json`; placeholders must be replaced)
- `bridge-data/{inbox,outbox,claimed,done,sent,history}/` — local queues;
  `history/<chat_id>.jsonl` gives Muse per-chat context
- `bridge-start.sh` / `bridge-run.sh` / `bridge-stop.sh` — keep-alive keeper
- `feishu-bridge.service` — optional systemd unit (installed but disabled
  until credentials exist)
- `logs/bridge.log`

## Deploy steps (for the deploying Muse)

1. **Install assets**
   ```bash
   mkdir -p ~/workspace/feishu-bridge/{bridge,bin,config,logs,run}
   cp assets/bridge/main.go assets/bridge/go.mod ~/workspace/feishu-bridge/bridge/
   cp assets/scripts/bridge-*.sh ~/workspace/feishu-bridge/
   chmod +x ~/workspace/feishu-bridge/bridge-*.sh
   cp assets/config/bridge.example.json ~/workspace/feishu-bridge/config/bridge.json
   chmod 600 ~/workspace/feishu-bridge/config/bridge.json
   ```
2. **Build** (needs Go ≥ 1.21; if `go` is missing, install a local toolchain
   under `~/workspace/feishu-bridge/tools/go`). On small tmpfs machines set
   `TMPDIR/GOTMPDIR/GOCACHE` to workspace dirs first.
   ```bash
   cd ~/workspace/feishu-bridge/bridge
   GOPROXY=https://proxy.golang.org,direct go mod tidy
   CGO_ENABLED=0 go build -o ../bin/feishubridge .
   ```
   Sanity check: running `bin/feishubridge` with the placeholder config must
   exit with code 2 and a "not filled" message. If it does not, stop.
3. **Get credentials from the user** (never invent or reuse someone else's):
   the user creates an enterprise self-built app at open.feishu.cn, enables
   the bot capability, and provides App ID + App Secret. Write them into
   `config/bridge.json`. Also set `bot_name` to the bot's exact display name
   (the daemon tries the app-info API first; many apps lack that permission,
   and the config value is the fallback used for group @ detection).
4. **Feishu console checklist** (user does this; all four are required):
   - Permissions: `im:message` (send/receive messages), plus
     `im:message.p2p_msg:readonly` for private chats, `im:resource`.
   - Event subscription: receiving mode = **long connection**
     (使用长连接接收事件）, subscribe `im.message.receive_v1`.
   - **Publish a new app version** after any permission/event change —
     changes do not take effect until published. This is the #1 cause of
     "group works, DM doesn't" or "nothing arrives".
   - Add the bot to the target group.
5. **Install the Muse wake-up hook**
   ```bash
   mkdir -p ~/hooks/scripts ~/hooks/state
   cp assets/hook/feishu-muse-inbox.sh ~/hooks/scripts/
   chmod +x ~/hooks/scripts/feishu-muse-inbox.sh
   ```
   Then register it with the hooks tools: id `feishu-muse-inbox`,
   poll interval 10s, worker prompt = the exact contents of
   `assets/hook/worker-prompt.md`. Dry-run it (empty inbox → `silent`;
   drop a fake inbox JSON → `wake`), then enable it.
   The script also carries the **self-healing watchdog** (see "Built-in
   behaviours"): because the hook is runtime-managed it survives VM /
   container replacements that wipe `/etc`, and its 10s poll doubles as
   the dead-man's switch for the daemon. It needs the filled unit file
   kept at `<install dir>/feishu-bridge.service` (step 6) to reinstall
   from. Repairs never run during dry runs.
6. **Start the daemon under systemd (primary keep-alive)**. A plain
   nohup keeper is NOT reliable enough: in production the keeper process
   died together with a dropped connection and the bridge stayed down for
   hours. Install the unit from `assets/scripts/feishu-bridge.service`
   (replace `__INSTALL_DIR__` with the absolute install dir and
   `__HOME_DIR__` with the user's home), then
   `systemctl daemon-reload && systemctl enable --now feishu-bridge`.
   The template's ExecStart pins `-root=<install dir>/bridge-data` and
   its `Environment=HOME=` pins the home directory — keep both: under
   systemd the daemon once derived a nested wrong data directory from
   a foreign HOME and wrote incoming messages where the hook never
   looks. Also keep a filled copy of the unit in the install dir: the
   watchdog reinstalls from it after a VM replacement wipes `/etc`.
   - **Proxy caveat**: if the agent shell has `http_proxy`/`https_proxy`
     set, copy those values into the unit's `Environment=` lines (template
     has them commented). systemd children get no shell environment, and
     without the proxy the Feishu WebSocket connect fails with a TLS
     handshake error.
   - **VM/container replacement wipes `/etc`**: the installed unit can
     disappear while `~/workspace` survives. Keep the filled unit file in
     the install dir and re-copy + `daemon-reload` + `enable` after any
     machine replacement.
   - Verify in `logs/bridge.log`: bot name line,
     `connected to wss://msg-frontier.feishu.cn/ws/v2`, ping/pong lines.
   The `bridge-start.sh` nohup keeper remains as a fallback for hosts
   without systemd — do not run both at once (two WebSocket clients for
   one app split the events between them).
7. **End-to-end test**: user @s the bot in the group (group chats only
   trigger on @; DMs always trigger). Expect: placeholder "Muse 正在思考…"
   appears, then turns into Muse's reply within ~a minute. Also test one
   attachment round (ask Muse in Feishu to send an image / a 2s video /
   a spreadsheet).
8. **(Optional, recommended on ephemeral hosts) Register the keep-alive
   scheduled check**: with the cron tools, add an interval job every
   30 minutes whose whole task is to run
   `systemctl is-active feishu-bridge`, report one line to the user only
   when the service is not active, and otherwise finish silently. Anchor
   its first run so its slots fall between any existing periodic
   heartbeat slots — see "Keep-alive scheduled check" under "Built-in
   behaviours" for why, and for the measured effect.

## Built-in behaviours worth knowing

- **Self-healing watchdog** (a dead-man's switch): the wake-up hook's
  script checks the whole
  supervision chain on every 10s live poll and repairs, rate-limited
  to one round per 120s: (1) systemd unit file missing — a VM /
  container replacement wipes `/etc` while the hook and `~/workspace`
  survive — reinstall from the install-dir copy, daemon-reload,
  enable; (2) service not active → start; (3) service active but dead
  inside — no established WebSocket connection for its pid, or the
  log heartbeat (WS ping/pong lines) stale over 300s → restart. A
  performed repair wakes the worker once (`feishu_bridge_repaired`)
  so the user hears one line about it; a healthy bridge stays silent.
  This layer exists because the Feishu protocol itself cannot be made
  loss-proof: Feishu long connections are push-only and never
  redeliver events missed while the client is offline, so supervision
  that heals in seconds is the strongest available substitute.
- **Catch-up sweep** (missed messages are recovered, not lost): because
  Feishu never redelivers, the daemon polls each known chat's history
  through the message-list API — 24h lookback shortly after startup,
  then a 30min lookback every 2 minutes — and enqueues user messages
  the live path never saw (marked `"recovered": true` in the inbox
  JSON, with their original send time). When several messages from
  one chat piled up during an outage, the sweep merges them into ONE
  catch-up entry (texts joined in time order, keyed by the latest
  message), so a single Muse turn answers the whole backlog instead
  of draining it turn by turn. A persistent seen registry
  (`bridge-data/seen.json`, seeded from the queue dirs) dedupes, and
  chat types (`bridge-data/chats.json`) are learned from live events;
  the chat-info API can also resolve them but needs the
  `im:chat:readonly` scope. A chat whose type is only guessed is
  treated as a group for gating, but its skipped messages are NOT
  marked seen — a guess must never eat a direct message. Scope note:
  listing **group** history needs `im:message.group_msg`; without it
  the sweep logs the refusal (throttled) and covers p2p chats only.
- **Keep-alive scheduled check** (recommended where the agent runtime
  is ephemeral): the watchdog can only run while the Muse runtime is
  alive, and on hosts where a VM / container replacement takes the
  runtime down together with the machine, the user-visible outage is
  however long the platform takes to hand the runtime back — the repair
  itself takes seconds, the wait for the runtime may not. A lightweight
  scheduled task that checks `systemctl is-active feishu-bridge` every
  ~30 minutes gives the platform a recurring due job, and due jobs
  appear to pull the runtime back sooner. In the reference deployment
  this cut the longest blind window in 24 hours from ~164 minutes to
  ~28 minutes: all 48 scheduled runs executed, none more than about a
  minute late, while on the baseline day several due jobs had been
  skipped entirely and one ran 27 minutes late. Deliberately offset the
  check from any existing periodic heartbeat (e.g. heartbeat at
  :16/:46, check at :01/:31) so a job falls due roughly every
  15 minutes. This is a mitigation, not a guarantee — a replacement
  that lands just after a check can still leave a wait of up to
  ~30 minutes — and the catch-up sweep, not this check, is what
  prevents message loss.
- **Anti-loop**: events with `sender_type=app` are dropped; every message
  the daemon sends is registered for 10 minutes and dropped if it echoes
  back. The bot never reacts to itself.
- **Group @ rule**: in groups only messages @-ing the bot are answered.
  Some clients send "@botname" as plain text with no mentions array — the
  daemon treats the literal `@<bot_name>` in the text as a mention too.
- **Attachments out**: outbox JSON may carry
  `attachments: [{path, kind}]` (kind image|video|file; path must be under
  `~/workspace`). Images go through `im/v1/images`, files through
  `im/v1/files` (`.xlsx`→file_type `xls`, `.mp4` video is sent as a `media`
  message).
- **Attachments in**: file / image / audio / media messages carry only a
  key in the event; the daemon downloads the bytes through the
  message-resource API at receive time (100 MB cap) into
  `bridge-data/files/<message_id>/` and lists them in the inbox JSON as
  `attachments: [{kind, name, path}]`, so the worker can read what the
  user sent. A failed download sets `error` on the entry instead of a
  path — the worker tells the user the file did not arrive rather than
  ignoring it. The catch-up sweep downloads the same way.
- **Video fix**: before sending a video the daemon remuxes it with
  `ffmpeg -movflags +faststart` and extracts a first-frame cover
  (`image_key`). Without faststart + cover, several Feishu mobile clients
  show the video as unplayable. Requires `ffmpeg` on PATH; failure falls
  back to sending the original file.
- **Config guard**: placeholder credentials make the daemon exit code 2 and
  the keeper refuses to restart-loop on that.

## Troubleshooting

- *Bridge was down for a while; messages sent meanwhile are gone* →
  Feishu long-connection events are **not** queued or redelivered for
  an offline client, but the catch-up sweep (see "Built-in behaviours")
  fetches missed messages from chat history once the bridge is back —
  for p2p chats they are answered late instead of lost. Group chats
  are covered only with the `im:message.group_msg` scope; without it,
  tell the user plainly which group window was lost and ask them to
  resend. Prevention is the
  systemd setup in step 6 (auto-start + Restart=always + proxy env)
  plus the watchdog in the hook script, which heals a wiped unit,
  a stopped service, or a dead-inside daemon within seconds of the
  machine being back; the keep-alive scheduled check (step 8)
  additionally shortens the wait for the runtime itself on hosts where
  a replacement takes the runtime down too — check `~/hooks/state/feishu-bridge-watchdog.state`
  and the hook log for its repair records before assuming a long
  outage went unnoticed.
- *Daemon running under systemd but WebSocket connect fails with
  `tls: first record does not look like a TLS handshake`* → the service
  is missing the proxy environment; see step 6.
- *A "Muse 正在思考…" placeholder never turns into the answer after a
  daemon restart* → fixed in the current source: the placeholder map is
  persisted to `bridge-data/pending.json` and reloaded on start. (Older
  builds kept it in memory only; on restart the reply still arrived, but
  as a separate message, leaving the placeholder stuck.)
- *Nothing arrives at all* → long-connection mode not selected, event not
  subscribed, or version not published. Check the daemon log: a healthy WS
  with zero `receive message` lines means Feishu is not pushing events.
- *Group works, DM silent* → missing `im:message.p2p_msg:readonly` scope;
  add it and publish a new version.
- *Bot name unknown / @ not recognised* → set `bot_name` in
  `config/bridge.json` to the exact display name.
- *Video shows black/unplayable* → confirm `ffmpeg` exists so the
  faststart+cover step runs.
- *Reply latency* → normal: 10s poll + Muse thinking; not a bug.
