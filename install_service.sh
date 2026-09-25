#!/bin/bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_USER="$(id -un)"
SERVICE_FILE="/etc/systemd/system/camhub.service"

sudo tee "$SERVICE_FILE" >/dev/null <<SERVICE
[Unit]
Description=CamHub Raspberry Pi Camera Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8080
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SERVICE

sudo systemctl daemon-reload
sudo systemctl enable --now camhub.service
sudo systemctl status camhub.service --no-pager
