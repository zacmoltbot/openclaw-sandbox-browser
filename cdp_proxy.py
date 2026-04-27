#!/usr/bin/env python3
"""
CDP reverse proxy for openclaw-sandbox-browser.

Chrome is started with --remote-debugging-address=127.0.0.1 so that its HTTP
endpoint is never exposed directly.  This proxy sits on 0.0.0.0:CDP_PORT and:

  - HTTP requests  → forwarded to Chrome; JSON responses have ws://127.0.0.1[:<port>]/
                     rewritten to ws://<PUBLIC_HOST>:<CDP_PORT>/ so that clients
                     in other containers can reach the WebSocket endpoint.
  - WebSocket upgrades → tunnelled directly to Chrome (raw TCP bidirectional pipe).
"""

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

_WS_RE = re.compile(r'ws://127\.0\.0\.1(?::\d+)?/')

# Track active tunnel count for observability
_active_tunnels = 0
_tunnel_lock = threading.Lock()


def _rewrite(data: bytes) -> bytes:
    text = data.decode("utf-8", errors="replace")
    text = _WS_RE.sub(f"ws://{PUBLIC_HOST}:{PUBLIC_PORT}/", text)
    return text.encode("utf-8")


def _pipe(src: socket.socket, dst: socket.socket, label: str) -> None:
    """Bidirectional pipe between src and dst. Logs disconnection."""
    import select
    try:
        while True:
            # Wait for src to have data before reading (handles non-blocking mode correctly)
            r, _, _ = select.select([src], [], [], 0.5)
            if not r:
                # Timeout: check if sockets are still connected
                continue
            chunk = src.recv(65536)
            if not chunk:
                break
            dst.sendall(chunk)
    except OSError:
        pass
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
        tunnel_id = id(self)
        print(f"[cdp_proxy][ws:{tunnel_id}] Opening tunnel to {CHROME_HOST}:{CHROME_PORT}", flush=True)

        try:
            chrome = socket.create_connection((CHROME_HOST, CHROME_PORT), timeout=10)
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] Chrome connection failed: {exc}", flush=True)
            self.send_error(503, "Chrome not reachable")
            return

        try:
            raw = f"GET {self.path} HTTP/1.1\r\n"
            for k, v in self.headers.items():
                raw += f"Host: 127.0.0.1\r\n" if k.lower() == "host" else f"{k}: {v}\r\n"
            raw += "\r\n"
            chrome.sendall(raw.encode())
        except Exception as exc:
            print(f"[cdp_proxy][ws:{tunnel_id}] Failed to send request to Chrome: {exc}", flush=True)
            chrome.close()
            self.send_error(502, "Failed to tunnel request")
            return

        client = self.connection
        client.setblocking(False)
        chrome.setblocking(False)

        with _tunnel_lock:
            _active_tunnels += 1
            count = _active_tunnels
        print(f"[cdp_proxy][ws:{tunnel_id}] Tunnel open (active={count})", flush=True)

        try:
            t1 = threading.Thread(target=_pipe, args=(client, chrome, f"{tunnel_id}-c2s"), daemon=True)
            t2 = threading.Thread(target=_pipe, args=(chrome, client, f"{tunnel_id}-s2c"), daemon=True)
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
            print(f"[cdp_proxy][ws:{tunnel_id}] Tunnel closed (active={_active_tunnels})", flush=True)

    def _proxy_http(self) -> None:
        url = f"http://{CHROME_HOST}:{CHROME_PORT}{self.path}"
        req = URLRequest(url, headers={"Host": "127.0.0.1"})
        try:
            with urlopen(req, timeout=10) as resp:
                body = resp.read()
                ct = resp.headers.get("Content-Type", "application/json")
                if "json" in ct.lower():
                    body = _rewrite(body)
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except Exception as exc:
            print(f"[cdp_proxy][http] Proxy failed for {self.path}: {exc}", flush=True)
            self.send_error(502, str(exc))

    def log_message(self, fmt, *args) -> None:
        # silenced; noise in tunnel mode
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
    print(f"[cdp_proxy] Starting threaded server on 0.0.0.0:{PUBLIC_PORT}, "
          f"chrome={CHROME_HOST}:{CHROME_PORT}, public_host={PUBLIC_HOST}", flush=True)
    server = _ThreadedHTTPServer(("0.0.0.0", PUBLIC_PORT), _Handler)
    server.timeout = None  # serveforever without blocking handle_error
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()