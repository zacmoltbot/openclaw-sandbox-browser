#!/usr/bin/env bash
set -euo pipefail

export DISPLAY=:1
export HOME=/tmp/openclaw-home
export XDG_CONFIG_HOME="${HOME}/.config"
export XDG_CACHE_HOME="${HOME}/.cache"

# Support OPENCLAW_*, MOLTBOT_*, and legacy CLAWDBOT_* env vars
CDP_PORT="${OPENCLAW_BROWSER_CDP_PORT:-${MOLTBOT_BROWSER_CDP_PORT:-${CLAWDBOT_BROWSER_CDP_PORT:-9222}}}"
VNC_PORT="${OPENCLAW_BROWSER_VNC_PORT:-${MOLTBOT_BROWSER_VNC_PORT:-${CLAWDBOT_BROWSER_VNC_PORT:-5900}}}"
NOVNC_PORT="${OPENCLAW_BROWSER_NOVNC_PORT:-${MOLTBOT_BROWSER_NOVNC_PORT:-${CLAWDBOT_BROWSER_NOVNC_PORT:-6080}}}"
ENABLE_NOVNC="${OPENCLAW_BROWSER_ENABLE_NOVNC:-${MOLTBOT_BROWSER_ENABLE_NOVNC:-${CLAWDBOT_BROWSER_ENABLE_NOVNC:-1}}}"
HEADLESS="${OPENCLAW_BROWSER_HEADLESS:-${MOLTBOT_BROWSER_HEADLESS:-${CLAWDBOT_BROWSER_HEADLESS:-0}}}"
# Hostname that clients (e.g. OpenClaw in another container) use to reach this service.
# Defaults to the Zeabur internal DNS name; override with OPENCLAW_BROWSER_PUBLIC_HOST if needed.
PUBLIC_HOST="${OPENCLAW_BROWSER_PUBLIC_HOST:-openclaw-sandbox-browser}"

echo "[entrypoint] Starting browser sandbox on CDP_PORT=${CDP_PORT} PUBLIC_HOST=${PUBLIC_HOST} HEADLESS=${HEADLESS}"

mkdir -p "${HOME}" "${HOME}/.chrome" "${XDG_CONFIG_HOME}" "${XDG_CACHE_HOME}"

echo "[entrypoint] Starting Xvfb..."
Xvfb :1 -screen 0 1280x800x24 -ac -nolisten tcp &
sleep 1

if [[ "${HEADLESS}" == "1" ]]; then
  CHROME_ARGS=(
    "--headless=new"
    "--disable-gpu"
  )
else
  CHROME_ARGS=()
fi

if [[ "${CDP_PORT}" -ge 65535 ]]; then
  CHROME_CDP_PORT="$((CDP_PORT - 1))"
else
  CHROME_CDP_PORT="$((CDP_PORT + 1))"
fi

CHROME_ARGS+=(
  "--remote-debugging-address=127.0.0.1"
  "--remote-debugging-port=${CHROME_CDP_PORT}"
  "--user-data-dir=${HOME}/.chrome"
  "--no-first-run"
  "--no-default-browser-check"
  "--disable-dev-shm-usage"
  "--disable-background-networking"
  "--disable-features=TranslateUI"
  "--disable-breakpad"
  "--disable-crash-reporter"
  "--metrics-recording-only"
  "--no-sandbox"
)

echo "[entrypoint] Starting Chromium on internal port ${CHROME_CDP_PORT}..."
chromium "${CHROME_ARGS[@]}" about:blank &
CHROME_PID=$!

for _ in $(seq 1 50); do
  if curl -sS --max-time 1 "http://127.0.0.1:${CHROME_CDP_PORT}/json/version" >/dev/null; then
    echo "[entrypoint] Chromium ready (internal port ${CHROME_CDP_PORT})"
    break
  fi
  sleep 0.1
done

echo "[entrypoint] Starting CDP proxy on port ${CDP_PORT}..."
CDP_PORT="${CDP_PORT}" CHROME_CDP_INTERNAL_PORT="${CHROME_CDP_PORT}" \
  OPENCLAW_BROWSER_PUBLIC_HOST="${PUBLIC_HOST}" \
  python3 /usr/local/bin/cdp_proxy.py &
PROXY_PID=$!

# Give proxy a moment to bind, then verify it responds
sleep 0.5

# Wait for proxy to be ready before declaring startup complete
echo "[entrypoint] Waiting for CDP proxy readiness..."
PROXY_READY=0
for _ in $(seq 1 30); do
  if curl -sS --max-time 1 "http://127.0.0.1:${CDP_PORT}/healthz" 2>/dev/null | grep -q "ok"; then
    echo "[entrypoint] CDP proxy ready on port ${CDP_PORT}"
    PROXY_READY=1
    break
  fi
  sleep 0.2
done

if [[ "${PROXY_READY}" == "0" ]]; then
  echo "[entrypoint] WARNING: CDP proxy healthz not responding after 6s, continuing anyway..."
fi

if [[ "${ENABLE_NOVNC}" == "1" && "${HEADLESS}" != "1" ]]; then
  echo "[entrypoint] Starting noVNC (VNC=${VNC_PORT} noVNC=${NOVNC_PORT})..."
  x11vnc -display :1 -rfbport "${VNC_PORT}" -shared -forever -nopw -localhost &
  websockify --web /usr/share/novnc/ "${NOVNC_PORT}" "localhost:${VNC_PORT}" &
fi

echo "[entrypoint] Startup complete. Chromium PID=${CHROME_PID} Proxy PID=${PROXY_PID}"

# Wait for any child to exit; in normal operation this means we stay alive while Chromium runs
wait -n
echo "[entrypoint] A child process exited, terminating."