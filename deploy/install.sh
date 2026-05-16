#!/usr/bin/env bash
# Social Pulse — server-side install script
# Run as: bash install.sh
set -euo pipefail

REPO="https://github.com/fborbon/social-pulse"
INSTALL_DIR="/opt/social-pulse"
SERVICE="social-pulse"
NGINX_SITE="/etc/nginx/sites-available/forwardforecasting"

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
echo "[5/6] Patching nginx config..."
BLOCK='
    # Social Pulse dashboard — injected by install.sh
    location /social-pulse/ {
        proxy_pass         http://127.0.0.1:8000/;
        proxy_http_version 1.1;
        proxy_set_header   Upgrade    \$http_upgrade;
        proxy_set_header   Connection "upgrade";
        proxy_set_header   Host       \$host;
        proxy_set_header   X-Real-IP  \$remote_addr;
        proxy_read_timeout 3600;
        proxy_send_timeout 3600;
    }'

if grep -q "social-pulse" "$NGINX_SITE" 2>/dev/null; then
  echo "      nginx block already present, skipping."
else
  # Insert before the closing brace of the server block
  sudo sed -i "s|}$|${BLOCK}\n}|" "$NGINX_SITE"
fi

sudo nginx -t && sudo systemctl reload nginx

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
