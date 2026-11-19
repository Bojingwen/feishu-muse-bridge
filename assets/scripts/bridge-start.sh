#!/usr/bin/env bash
# Start the Feishu <-> Muse bridge in the background (keep-alive via bridge-run.sh).
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$ROOT/logs" "$ROOT/run"
if [ -f "$ROOT/run/bridge-keeper.pid" ] && kill -0 "$(cat "$ROOT/run/bridge-keeper.pid")" 2>/dev/null; then
  echo "bridge already running, keeper pid=$(cat "$ROOT/run/bridge-keeper.pid")"
  exit 0
fi
# Refuse placeholder config up front with a clear message.
if grep -q "REPLACE_WITH_\|cli_xxxxxxxxxxxxxxxx" "$ROOT/config/bridge.json"; then
  echo "config/bridge.json 还是占位符：先填 lark_app_id / lark_app_secret 再启动。"
  exit 2
fi
nohup "$ROOT/bridge-run.sh" > "$ROOT/logs/bridge-keeper.out" 2>&1 &
echo "bridge keeper started pid=$!  log: $ROOT/logs/bridge.log"
sleep 3
tail -n 15 "$ROOT/logs/bridge.log" 2>/dev/null || true
