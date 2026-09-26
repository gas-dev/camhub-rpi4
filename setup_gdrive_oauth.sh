#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

REMOTE="gdrive"
if [ -f "$APP_DIR/config.json" ]; then
  REMOTE="$(python3 - <<'PY'
import json
from pathlib import Path
p = Path("config.json")
try:
    print(json.loads(p.read_text(encoding="utf-8")).get("drive_remote", "gdrive"))
except Exception:
    print("gdrive")
PY
)"
fi

if ! command -v rclone >/dev/null 2>&1; then
  echo "ERROR: rclone is not installed."
  exit 1
fi

echo "CamHub Google Drive dedicated OAuth setup"
echo "Remote: $REMOTE"
echo
echo "Create a Google Cloud OAuth Client ID of type Desktop app first."
echo "The Client ID and secret entered here remain only in the local rclone config."
echo

read -r -p "Google OAuth Client ID: " CLIENT_ID
if [ -z "$CLIENT_ID" ]; then
  echo "ERROR: Client ID cannot be empty."
  exit 1
fi

read -r -s -p "Google OAuth Client Secret: " CLIENT_SECRET
echo
if [ -z "$CLIENT_SECRET" ]; then
  echo "ERROR: Client Secret cannot be empty."
  exit 1
fi

CONFIG_FILE="$(rclone config file 2>/dev/null | awk '/^\// {print; exit}')"
if [ -n "${CONFIG_FILE:-}" ] && [ -f "$CONFIG_FILE" ]; then
  BACKUP="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
  cp "$CONFIG_FILE" "$BACKUP"
  chmod 600 "$BACKUP" || true
  echo "Backup created: $BACKUP"
fi

if rclone listremotes | grep -qx "${REMOTE}:"; then
  echo "Updating existing remote $REMOTE..."
  rclone config update "$REMOTE" \
    client_id "$CLIENT_ID" \
    client_secret "$CLIENT_SECRET"
else
  echo "Creating remote $REMOTE..."
  rclone config create "$REMOTE" drive \
    scope drive \
    client_id "$CLIENT_ID" \
    client_secret "$CLIENT_SECRET"
fi

unset CLIENT_SECRET

echo
echo "OAuth browser authorization is now required."
echo "On a headless Raspberry Pi, follow the remote authorization instructions printed by rclone."
echo

rclone config reconnect "${REMOTE}:"

ROOT="CamHub"
if [ -f "$APP_DIR/config.json" ]; then
  ROOT="$(python3 - <<'PY'
import json
from pathlib import Path
p = Path("config.json")
try:
    print(json.loads(p.read_text(encoding="utf-8")).get("drive_root", "CamHub"))
except Exception:
    print("CamHub")
PY
)"
fi

echo
echo "Ensuring Drive root exists: ${REMOTE}:${ROOT}"
rclone mkdir "${REMOTE}:${ROOT}" \
  --tpslimit 8 \
  --tpslimit-burst 8

echo "Testing Google Drive..."
rclone lsd "${REMOTE}:${ROOT}" \
  --max-depth 1 \
  --tpslimit 8 \
  --tpslimit-burst 8

echo
echo "Dedicated OAuth configuration completed successfully."
echo "Restart CamHub with:"
echo "  sudo systemctl restart camhub"
echo
echo "Then open the dashboard and press 'Test Google Drive'."
