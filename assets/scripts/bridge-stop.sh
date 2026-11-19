#!/usr/bin/env bash
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$ROOT/run/bridge-keeper.pid" ]; then
  KP="$(cat "$ROOT/run/bridge-keeper.pid")"
  pkill -TERM -P "$KP" 2>/dev/null || true
  kill -TERM "$KP" 2>/dev/null || true
  rm -f "$ROOT/run/bridge-keeper.pid"
  echo "stopped bridge keeper $KP"
else
  echo "no bridge keeper pid file"
fi
pkill -TERM -f "$ROOT/bin/feishubridge" 2>/dev/null || true
