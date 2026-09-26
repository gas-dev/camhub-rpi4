#!/bin/bash
set -u

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

REMOTE="gdrive"
ROOT="CamHub"

if [ -f "$APP_DIR/config.json" ]; then
  readarray -t VALUES < <(python3 - <<'PY'
import json
from pathlib import Path
try:
    cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
except Exception:
    cfg = {}
print(cfg.get("drive_remote", "gdrive"))
print(cfg.get("drive_root", "CamHub"))
PY
)
  REMOTE="${VALUES[0]:-gdrive}"
  ROOT="${VALUES[1]:-CamHub}"
fi

echo "CamHub cloud diagnostic"
echo "Remote: $REMOTE"
echo "Root:   $ROOT"
echo

if ! command -v rclone >/dev/null 2>&1; then
  echo "ERROR: rclone is not installed."
  exit 1
fi

rclone version | head -n 2
echo

CONFIG_FILE="$(rclone config file 2>/dev/null | awk '/^\// {print; exit}')"
echo "Config: ${CONFIG_FILE:-unknown}"

CUSTOM="NO"
if [ -n "${CONFIG_FILE:-}" ] && [ -f "$CONFIG_FILE" ]; then
  CUSTOM="$(python3 - "$CONFIG_FILE" "$REMOTE" <<'PY'
import configparser, sys
p, remote = sys.argv[1], sys.argv[2]
cfg = configparser.RawConfigParser()
cfg.read(p, encoding="utf-8")
print("YES" if cfg.has_section(remote) and cfg.get(remote, "client_id", fallback="").strip() else "NO")
PY
)"
fi

echo "Dedicated OAuth client: $CUSTOM"
echo

echo "Remote connectivity:"
rclone about "${REMOTE}:" \
  --tpslimit 8 \
  --tpslimit-burst 8 2>&1
ABOUT_RC=$?
echo

echo "Configured root:"
rclone lsd "${REMOTE}:${ROOT}" \
  --max-depth 1 \
  --tpslimit 8 \
  --tpslimit-burst 8 2>&1
LSD_RC=$?
echo

if [ "$ABOUT_RC" -eq 0 ] && [ "$LSD_RC" -eq 0 ]; then
  echo "RESULT: Google Drive OK"
  exit 0
fi

echo "RESULT: Google Drive diagnostic FAILED"
exit 2
