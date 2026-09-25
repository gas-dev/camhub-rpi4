#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Installing CamHub in: $APP_DIR"
sudo apt update
sudo apt install -y python3 python3-venv ffmpeg rclone

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
mkdir -p "$APP_DIR/data"

if [ ! -f "$APP_DIR/config.json" ]; then
  cp "$APP_DIR/config.example.json" "$APP_DIR/config.json"
fi

echo
echo "Installation complete."
echo "Start CamHub manually with:"
echo "  $APP_DIR/.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080 --app-dir $APP_DIR"
echo
echo "Then open: http://RASPBERRY_IP:8080"
echo "Configure Google Drive with: rclone config"
