#!/usr/bin/env bash
# Feishu <-> Muse bridge keeper: restarts feishubridge if it exits.
# The bridge uses Feishu long-connection (WebSocket): no public callback URL.
# Stop with ./bridge-stop.sh
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/bin/feishubridge"
CONF="$ROOT/config/bridge.json"
LOG="$ROOT/logs/bridge.log"
mkdir -p "$ROOT/logs" "$ROOT/run"
echo $$ > "$ROOT/run/bridge-keeper.pid"
echo "[bridge-keeper] started $(date '+%F %T %Z')" >> "$LOG"
attempt=0
while true; do
  attempt=$((attempt+1))
  echo "[bridge-keeper] starting feishubridge attempt #$attempt $(date '+%F %T %Z')" >> "$LOG"
  "$BIN" -conf="$CONF" >> "$LOG" 2>&1
  code=$?
  echo "[bridge-keeper] feishubridge exited code=$code $(date '+%F %T %Z'), restart in 5s" >> "$LOG"
  if [ "$code" = "2" ]; then
    echo "[bridge-keeper] config not filled (exit 2), not restarting. Fill $CONF first." >> "$LOG"
    break
  fi
  sleep 5
done
