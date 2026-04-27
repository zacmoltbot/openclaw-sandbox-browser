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
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request as URLRequest
from urllib.request import urlopen

CHROME_HOST = "127.0.0.1"
CHROME_PORT = int(os.environ["CHROME_CDP_INTERNAL_PORT"])
PUBLIC_HOST = os.environ.get("OPENCLAW_BROWSER_PUBLIC_HOST", "openclaw-sandbox-browser")
PUBLIC_PORT = int(os.environ["CDP_PORT"])

# Runtime fingerprint — populated at startup for positive identification
try:
    import subprocess
    _GIT_SHA    = subprocess.check_output(
        ["git", "rev-parse", "--short=8", "HEAD"],
        stderr=subprocess.DEVNULL).strip().decode()
    _GIT_BRANCH = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        stderr=subprocess.DEVNULL).strip().decode()
except Exception:
    _GIT_SHA    = "unknown"
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


def _pipe(src: socket.socket, dst: socket.socket, label: str) -> None:
    """Bidirectional pipe between src and dst. Logs disconnection and timeouts."""
    import select
    select_timeouts = 0
    select_lock = threading.Lock()
    try:
        while True:
            r, _, _ = select.select([src], [], [], 0.5)
            if not r:
                select_timeouts += 1
                if select_timeouts % 20 == 0:  # log every 10s of idle
                    print(f"[cdp_proxy][pipe:{label}] select timeout #{select_timeouts} "
                          f"(idle {select_timeouts * 0.5:.0f}s) "
                          f"src_so={src.getsockname()} dst_so={dst.getsockname()}",
                          flush=True)
                continue
            select_timeouts = 0
            try:
                chunk = src.recv(65536)
            except BlockingIOError:
                # Non-blocking recv with no data — should not happen after select
                continue
            if not chunk:
                print(f"[cdp_proxy][pipe:{label}] EOF received, closing", flush=True)
                break
            try:
                dst.sendall(chunk)
            except BlockingIOError:
                # Kernel send buffer full —短暂背压；继续尝试
                try:
                    import select as _s
                    while True:
                        _, w, _ = _s.select([], [dst], [], 0.5)
                        if w:
                            try:
                                dst.sendall(chunk)
                                break
                            except BlockingIOError:
                                pass
                        else:
                            select_timeouts += 1
                            if select_timeouts % 10 == 0:
                                print(f"[cdp_proxy][pipe:{label}] send buffer full "
                                      f"for {select_timeouts * 0.5:.0f}s, still trying",
                                      flush=True)
                finally:
                    del _s
    except OSError as exc:
        print(f"[cdp_proxy][pipe:{label}] OSError: {exc}", flush=True)
    except Exception as exc:
        print(f"[cdp_proxy][pipe:{label}] Error: {exc}", flush=True)
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class _Handler(BaseHTTPRequestHandler):
    server_version = "CDP-Proxy/1.0"

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
            with urlopen(url, timeout=3) as resp:
                if resp.status == 200:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"ok")
                    return
        except Exception:
            pass
        self.send_error(503, "Chrome not reachable")

    def _tunnel_ws(self) -> None:
        global _active_tunnels
        tunnel_id = f"{os.getpid()}-{id(self) & 0xFFFFFF:06x}"
        ws_path   = self.path          # e.g. /devtools/browser/... or /devtools/page/...
        ws_type   = "browser" if "/browser/" in ws_path else "page" if "/page/" in ws_path else "unknown"

        print(f"[cdp_proxy][ws:{tunnel_id}] UPGRADE_REQUEST path={ws_path} "
              f"type={ws_type} client={self.client_address} "
              f"t={_now()}", flush=True)

        try:
            chrome = socket.create_connection((CHROME_HOST, CHROME_PORT), timeout=10)
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] CHROME_CONNECT_FAIL: {exc} "
                  f"t={_now()}", flush=True)
            self.send_error(503, "Chrome not reachable")
            return

        try:
            raw = f"GET {self.path} HTTP/1.1\r\n"
            for k, v in self.headers.items():
                raw += f"Host: 127.0.0.1\r\n" if k.lower() == "host" else f"{k}: {v}\r\n"
            raw += "\r\n"
            chrome.sendall(raw.encode())
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] SEND_REQUEST_FAIL: {exc} "
                  f"t={_now()}", flush=True)
            chrome.close()
            self.send_error(502, "Failed to tunnel request")
            return

        client = self.connection
        client.setblocking(False)
        chrome.setblocking(False)

        with _tunnel_lock:
            _active_tunnels += 1
            count = _active_tunnels

        print(f"[cdp_proxy][ws:{tunnel_id}] TUNNEL_OPEN "
              f"type={ws_type} active={count} t={_now()}", flush=True)

        try:
            t1 = threading.Thread(target=_pipe,
                                  args=(client, chrome, f"{tunnel_id}-c2s"),
                                  daemon=True)
            t2 = threading.Thread(target=_pipe,
                                  args=(chrome, client, f"{tunnel_id}-s2c"),
                                  daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        finally:
            with _tunnel_lock:
                _active_tunnels -= 1
            try:
                chrome.close()
            except OSError:
                pass
            print(f"[cdp_proxy][ws:{tunnel_id}] TUNNEL_CLOSE "
                  f"active={_active_tunnels} t={_now()}", flush=True)

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
            with urlopen(req, timeout=10) as resp:
                body = resp.read()
                ct = resp.headers.get("Content-Type", "")
                # Rewrite ws:// URLs for any /json/* path or any JSON content type
                rewrite = (self.path.startswith("/json/")
                           or "json" in ct.lower())
                if rewrite:
                    body = _rewrite(body)
                    rewrote = "rewritten"
                else:
                    rewrote = "skipped"
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                print(f"[cdp_proxy][http] {resp.status} {self.path} "
                      f"ct={ct!r} rewrite={rewrote} size={len(body)} "
                      f"t={_now()}", flush=True)
        except Exception as exc:
            print(f"[cdp_proxy][http] PROXY_FAIL {self.path}: {exc} "
                  f"t={_now()}", flush=True)
            self.send_error(502, str(exc))

    def log_message(self, fmt, *args) -> None:
        # Silenced for WS tunnel noise; HTTP paths are logged in _proxy_http
        pass


class _ThreadedHTTPServer(HTTPServer):
    """HTTPServer subclass that handles each request in a new thread."""
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
    print(f"[cdp_proxy] === STARTUP { _RUNTIME_FINGERPRINT} ===", flush=True)
    print(f"[cdp_proxy] Starting threaded server on 0.0.0.0:{PUBLIC_PORT}, "
          f"chrome={CHROME_HOST}:{CHROME_PORT}, public_host={PUBLIC_HOST}", flush=True)
    server = _ThreadedHTTPServer(("0.0.0.0", PUBLIC_PORT), _Handler)
    server.timeout = None  # serveforever without blocking handle_error
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
