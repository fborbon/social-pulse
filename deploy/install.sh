#!/usr/bin/env bash
# Social Pulse — server-side install script
# Run as: bash install.sh
set -euo pipefail

REPO="https://github.com/fborbon/social-pulse"
INSTALL_DIR="/opt/social-pulse"
SERVICE="social-pulse"

echo "=== Social Pulse installer ==="

# ── 1. Clone / update repo ────────────────────────────────────────────────────
if [ -d "$INSTALL_DIR/.git" ]; then
  echo "[1/6] Updating existing repo..."
  git -C "$INSTALL_DIR" pull --ff-only
else
  echo "[1/6] Cloning repo to $INSTALL_DIR..."
  sudo git clone "$REPO" "$INSTALL_DIR"
  sudo chown -R ubuntu:ubuntu "$INSTALL_DIR"
fi

# ── 2. Python deps ────────────────────────────────────────────────────────────
echo "[2/6] Installing Python dependencies..."
pip3 install --quiet --break-system-packages \
  fastapi "uvicorn[standard]" aiohttp feedparser pydantic \
  pydantic-settings "strawberry-graphql[fastapi]" anthropic \
  httpx python-dotenv aiofiles aiosqlite apscheduler \
  "scikit-learn" numpy atproto

# ── 3. .env ───────────────────────────────────────────────────────────────────
if [ ! -f "$INSTALL_DIR/.env" ]; then
  echo "[3/6] Creating .env from template — fill in your API keys..."
  cp "$INSTALL_DIR/.env.example" "$INSTALL_DIR/.env"
  echo "      Edit $INSTALL_DIR/.env and re-run this script, or start the service now."
else
  echo "[3/6] .env already exists, skipping."
fi

# ── 4. systemd service ────────────────────────────────────────────────────────
echo "[4/6] Installing systemd service..."
sudo cp "$INSTALL_DIR/deploy/social-pulse.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE

# ── 5. nginx location block ───────────────────────────────────────────────────
# The /social-pulse/ locations (including the login-protected /collect/now and the rate-limited
# /search) live in the main-site config owned by the sp500-correlation-bot repo
# (nginx/forwardforecasting.conf, installed with a drift guard). Never patch the live file here.
echo "[5/6] nginx: managed by the main-site config repo, nothing to do."

# ── 6. Start service ──────────────────────────────────────────────────────────
echo "[6/6] Starting Social Pulse..."
sudo systemctl restart $SERVICE
sleep 4
sudo systemctl status $SERVICE --no-pager

echo ""
echo "=== Done ==="
echo "Dashboard : https://www.forwardforecasting.eu/social-pulse/"
echo "Health    : https://www.forwardforecasting.eu/social-pulse/health"
echo "Logs      : journalctl -u social-pulse -f"
