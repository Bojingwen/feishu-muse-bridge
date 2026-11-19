#!/usr/bin/env bash
# Polls the Feishu bridge inbox; wakes a Muse worker when a message waits.
# Also carries a bridge watchdog (a dead-man's switch): every live run
# checks the bridge's supervision chain and repairs what VM
# replacements / process deaths break, so a dropped Feishu connection
# heals itself within seconds:
#   1. systemd unit file missing (a VM replacement wipes /etc)
#      -> reinstall from the workspace copy, daemon-reload, enable.
#   2. service not active -> start it.
#   3. service active but dead inside: no established WS connection for
#      its pid (checked once it has been up >60s), or the bridge log
#      (its heartbeat: the daemon logs WS ping/pong) has not advanced
#      for >300s -> restart it.
# Repairs are rate-limited to one round per 120s, never run during dry
# runs, and a performed repair wakes the worker once so it can tell the
# user in one line. Contains no secrets: it only looks at local files.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE_DIR="$HOME/workspace/feishu-bridge"
INBOX="$BRIDGE_DIR/bridge-data/inbox"
UNIT_SRC="$BRIDGE_DIR/feishu-bridge.service"
UNIT_DST="/etc/systemd/system/feishu-bridge.service"
LOG_FILE="$BRIDGE_DIR/logs/bridge.log"
STATE_DIR="$HOME/hooks/state"
WD_STATE="$STATE_DIR/feishu-bridge-watchdog.state"
mkdir -p "$STATE_DIR"

repair_note=""
if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ] && command -v systemctl >/dev/null 2>&1; then
  now="$(date +%s)"
  last=0
  if [ -f "$WD_STATE" ]; then
    last="$(cat "$WD_STATE" 2>/dev/null || echo 0)"
    case "$last" in ''|*[!0-9]*) last=0 ;; esac
  fi
  if [ $((now - last)) -ge 120 ]; then
    problems=()
    if [ ! -f "$UNIT_DST" ] && [ -f "$UNIT_SRC" ]; then
      if cp "$UNIT_SRC" "$UNIT_DST" 2>/dev/null; then
        systemctl daemon-reload 2>/dev/null || true
        systemctl enable feishu-bridge >/dev/null 2>&1 || true
        problems+=("unit file missing (VM replaced?) - reinstalled and enabled")
      fi
    fi
    if ! systemctl is-active --quiet feishu-bridge 2>/dev/null; then
      if systemctl start feishu-bridge 2>/dev/null; then
        problems+=("service was down - started")
      else
        problems+=("service down and start FAILED - needs attention")
      fi
    else
      pid="$(systemctl show feishu-bridge -p MainPID --value 2>/dev/null || echo 0)"
      entered="$(systemctl show feishu-bridge -p ActiveEnterTimestamp --value 2>/dev/null || true)"
      entered_s=0
      if [ -n "$entered" ]; then
        entered_s="$(date -d "$entered" +%s 2>/dev/null || echo 0)"
      fi
      conn_ok=1
      if [ "${pid:-0}" != "0" ] && command -v ss >/dev/null 2>&1; then
        if ! ss -tnp 2>/dev/null | grep -q "pid=$pid,"; then
          conn_ok=0
        fi
      fi
      if [ "$conn_ok" = "0" ] && [ "$entered_s" -gt 0 ] && [ $((now - entered_s)) -gt 60 ]; then
        if systemctl restart feishu-bridge 2>/dev/null; then
          problems+=("WS connection gone while service stayed up - restarted")
        else
          problems+=("WS connection gone and restart FAILED - needs attention")
        fi
      elif [ -f "$LOG_FILE" ]; then
        mtime="$(stat -c %Y "$LOG_FILE" 2>/dev/null || echo 0)"
        if [ "$mtime" -gt 0 ] && [ $((now - mtime)) -gt 300 ]; then
          if systemctl restart feishu-bridge 2>/dev/null; then
            problems+=("heartbeat stale $((now - mtime))s - restarted")
          else
            problems+=("heartbeat stale and restart FAILED - needs attention")
          fi
        fi
      fi
    fi
    if [ ${#problems[@]} -gt 0 ]; then
      echo "$now" > "$WD_STATE"
      repair_note="$(IFS='; '; echo "${problems[*]}")"
      log "bridge watchdog repair" "{\"detail\":\"$repair_note\"}"
    fi
  fi
fi

found=""
if [ -d "$INBOX" ]; then
  found="$(find "$INBOX" -maxdepth 1 -name '*.json' -print -quit 2>/dev/null || true)"
fi
if [ -n "$found" ]; then
  if [ -n "$repair_note" ]; then
    wake "feishu_message_waiting" "{\"file\":\"$found\",\"bridge_repair\":\"$repair_note\"}"
  else
    wake "feishu_message_waiting" "{\"file\":\"$found\"}"
  fi
elif [ -n "$repair_note" ]; then
  wake "feishu_bridge_repaired" "{\"detail\":\"$repair_note\"}"
else
  silent "inbox empty, bridge healthy" '{}'
fi
