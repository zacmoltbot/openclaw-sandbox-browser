#!/usr/bin/env python3
"""
CDP reverse proxy for openclaw-sandbox-browser.

Chrome is started with --remote-debugging-address=127.0.0.1 so that its HTTP
endpoint is never exposed directly.  This proxy sits on 0.0.0.0:CDP_PORT and:

  - HTTP requests  → forwarded to Chrome; responses that look like CDP JSON
                     (or are for /json/* paths) have ws://127.0.0.1[:<port>]/
                     rewritten to ws://<PUBLIC_HOST>:<CDP_PORT>/ so that clients
                     in other containers can reach the WebSocket endpoint.
  - WebSocket upgrades → tunnelled directly to Chrome (raw TCP bidirectional pipe).
"""

import datetime
import os
import re
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.request import Request as URLRequest
from urllib.request import urlopen

CHROME_HOST = "127.0.0.1"
CHROME_PORT = int(os.environ["CHROME_CDP_INTERNAL_PORT"])
PUBLIC_HOST = os.environ.get("OPENCLAW_BROWSER_PUBLIC_HOST", "openclaw-sandbox-browser")
PUBLIC_PORT = int(os.environ["CDP_PORT"])
HTTP_PROXY_TIMEOUT = float(os.environ.get("CDP_PROXY_HTTP_TIMEOUT", "10"))
HEALTHCHECK_TIMEOUT = float(os.environ.get("CDP_PROXY_HEALTHCHECK_TIMEOUT", "3"))
WS_CONNECT_TIMEOUT = float(os.environ.get("CDP_PROXY_WS_CONNECT_TIMEOUT", "10"))
WS_IDLE_LOG_INTERVAL = int(os.environ.get("CDP_PROXY_WS_IDLE_LOG_INTERVAL", "20"))
WS_SEND_TIMEOUT = float(os.environ.get("CDP_PROXY_WS_SEND_TIMEOUT", "30"))

# Runtime fingerprint — populated at startup for positive identification
try:
    _GIT_SHA = subprocess.check_output(
        ["git", "rev-parse", "--short=8", "HEAD"],
        stderr=subprocess.DEVNULL).strip().decode()
    _GIT_BRANCH = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        stderr=subprocess.DEVNULL).strip().decode()
except Exception:
    _GIT_SHA = "unknown"
    _GIT_BRANCH = "unknown"

_RUNTIME_FINGERPRINT = (
    f"cdp_proxy pid={os.getpid()} git={_GIT_BRANCH}@{_GIT_SHA} "
    f"started={datetime.datetime.utcnow().isoformat(timespec='milliseconds')}Z "
    f"chrome={CHROME_HOST}:{CHROME_PORT} public={PUBLIC_HOST}:{PUBLIC_PORT}"
)

_WS_RE = re.compile(r'ws://127\.0\.0\.1(?::\d+)?/')

# Track active tunnel count for observability
_active_tunnels = 0
_tunnel_lock = threading.Lock()


def _now() -> str:
    """ISO timestamp with millisecond precision, UTC."""
    return datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def _rewrite(data: bytes) -> bytes:
    text = data.decode("utf-8", errors="replace")
    text = _WS_RE.sub(f"ws://{PUBLIC_HOST}:{PUBLIC_PORT}/", text)
    return text.encode("utf-8")


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _pipe(src: socket.socket, dst: socket.socket, label: str, stop: threading.Event) -> None:
    """Bidirectional pipe between src and dst with bounded send blocking."""
    idle_ticks = 0
    try:
        while not stop.is_set():
            try:
                chunk = src.recv(65536)
            except socket.timeout:
                idle_ticks += 1
                if idle_ticks % WS_IDLE_LOG_INTERVAL == 0:
                    print(
                        f"[cdp_proxy][pipe:{label}] recv timeout #{idle_ticks} "
                        f"(idle {idle_ticks:.0f}s) t={_now()}",
                        flush=True,
                    )
                continue
            except BlockingIOError:
                continue

            idle_ticks = 0
            if not chunk:
                print(f"[cdp_proxy][pipe:{label}] EOF received, closing", flush=True)
                break

            try:
                dst.sendall(chunk)
            except socket.timeout:
                print(
                    f"[cdp_proxy][pipe:{label}] send timeout after {WS_SEND_TIMEOUT}s, closing "
                    f"t={_now()}",
                    flush=True,
                )
                break
            except BlockingIOError:
                print(f"[cdp_proxy][pipe:{label}] unexpected non-blocking send stall", flush=True)
                break
    except OSError as exc:
        print(f"[cdp_proxy][pipe:{label}] OSError: {exc}", flush=True)
    except Exception as exc:
        print(f"[cdp_proxy][pipe:{label}] Error: {exc}", flush=True)
    finally:
        stop.set()
        _close_socket(src)
        _close_socket(dst)


class _Handler(BaseHTTPRequestHandler):
    server_version = "CDP-Proxy/1.1"

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._healthz()
        elif self.headers.get("Upgrade", "").lower() == "websocket":
            self._tunnel_ws()
        else:
            self._proxy_http()

    def _healthz(self) -> None:
        """Readiness probe: checks that Chrome is reachable."""
        try:
            url = f"http://{CHROME_HOST}:{CHROME_PORT}/json/version"
            with urlopen(url, timeout=HEALTHCHECK_TIMEOUT) as resp:
                if resp.status == 200:
                    body = f"ok active_tunnels={_active_tunnels}\n".encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
        except Exception as exc:
            print(f"[cdp_proxy][healthz] FAIL: {exc} t={_now()}", flush=True)
        self.send_error(503, "Chrome not reachable")

    def _tunnel_ws(self) -> None:
        global _active_tunnels
        tunnel_id = f"{os.getpid()}-{id(self) & 0xFFFFFF:06x}"
        ws_path = self.path
        ws_type = "browser" if "/browser/" in ws_path else "page" if "/page/" in ws_path else "unknown"

        print(
            f"[cdp_proxy][ws:{tunnel_id}] UPGRADE_REQUEST path={ws_path} "
            f"type={ws_type} client={self.client_address} "
            f"t={_now()}",
            flush=True,
        )

        try:
            chrome = socket.create_connection((CHROME_HOST, CHROME_PORT), timeout=WS_CONNECT_TIMEOUT)
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] CHROME_CONNECT_FAIL: {exc} t={_now()}", flush=True)
            self.send_error(503, "Chrome not reachable")
            return

        try:
            raw = f"GET {self.path} HTTP/1.1\r\n"
            for k, v in self.headers.items():
                raw += f"Host: 127.0.0.1\r\n" if k.lower() == "host" else f"{k}: {v}\r\n"
            raw += "\r\n"
            chrome.sendall(raw.encode())
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] SEND_REQUEST_FAIL: {exc} t={_now()}", flush=True)
            _close_socket(chrome)
            self.send_error(502, "Failed to tunnel request")
            return

        client = self.connection
        client.settimeout(1.0)
        chrome.settimeout(1.0)

        with _tunnel_lock:
            _active_tunnels += 1
            count = _active_tunnels

        print(f"[cdp_proxy][ws:{tunnel_id}] TUNNEL_OPEN type={ws_type} active={count} t={_now()}", flush=True)

        stop = threading.Event()
        try:
            t1 = threading.Thread(
                target=_pipe,
                args=(client, chrome, f"{tunnel_id}-c2s", stop),
                daemon=True,
            )
            t2 = threading.Thread(
                target=_pipe,
                args=(chrome, client, f"{tunnel_id}-s2c", stop),
                daemon=True,
            )
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        finally:
            stop.set()
            with _tunnel_lock:
                _active_tunnels -= 1
                count = _active_tunnels
            _close_socket(chrome)
            _close_socket(client)
            print(f"[cdp_proxy][ws:{tunnel_id}] TUNNEL_CLOSE active={count} t={_now()}", flush=True)

    def _proxy_http(self) -> None:
        """HTTP proxy: forward to Chrome, rewrite ws:// URLs in JSON bodies.

        CRITICAL: The ws:// URL rewrite is applied whenever the request path
        is under /json/ (the CDP discovery API) — NOT only when Content-Type
        contains 'json'.  Chrome has been observed to serve /json/list with
        Content-Type: text/plain, and without rewriting the client receives
        ws://127.0.0.1:<internal_port>/ URLs that point directly at Chrome,
        bypassing the proxy entirely and causing attach timeouts.
        """
        url = f"http://{CHROME_HOST}:{CHROME_PORT}{self.path}"
        req = URLRequest(url, headers={"Host": "127.0.0.1"})
        try:
            with urlopen(req, timeout=HTTP_PROXY_TIMEOUT) as resp:
                body = resp.read()
                ct = resp.headers.get("Content-Type", "")
                rewrite = self.path.startswith("/json/") or "json" in ct.lower()
                if rewrite:
                    body = _rewrite(body)
                    rewrote = "rewritten"
                else:
                    rewrote = "skipped"
                self.send_response(resp.status)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                print(
                    f"[cdp_proxy][http] {resp.status} {self.path} "
                    f"ct={ct!r} rewrite={rewrote} size={len(body)} "
                    f"t={_now()}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[cdp_proxy][http] PROXY_FAIL {self.path}: {exc} t={_now()}", flush=True)
            self.send_error(502, str(exc))

    def log_message(self, fmt, *args) -> None:
        # Silenced for WS tunnel noise; HTTP paths are logged in _proxy_http
        pass


class _ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """HTTP server that handles each request in a separate daemon thread."""

    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, socket.error):
            # Broken pipe / client disconnected — not worth logging
            pass
        else:
            print(f"[cdp_proxy][server] Error: {exc}", flush=True)


if __name__ == "__main__":
    print(f"[cdp_proxy] === STARTUP {_RUNTIME_FINGERPRINT} ===", flush=True)
    print(
        f"[cdp_proxy] Starting threaded server on 0.0.0.0:{PUBLIC_PORT}, "
        f"chrome={CHROME_HOST}:{CHROME_PORT}, public_host={PUBLIC_HOST}, "
        f"http_timeout={HTTP_PROXY_TIMEOUT}s healthz_timeout={HEALTHCHECK_TIMEOUT}s "
        f"ws_connect_timeout={WS_CONNECT_TIMEOUT}s ws_send_timeout={WS_SEND_TIMEOUT}s",
        flush=True,
    )
    server = _ThreadedHTTPServer(("0.0.0.0", PUBLIC_PORT), _Handler)
    server.timeout = None
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
