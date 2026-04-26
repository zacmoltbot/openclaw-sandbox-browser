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
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request as URLRequest
from urllib.request import urlopen

CHROME_HOST = "127.0.0.1"
CHROME_PORT = int(os.environ["CHROME_CDP_INTERNAL_PORT"])
PUBLIC_HOST = os.environ.get("OPENCLAW_BROWSER_PUBLIC_HOST", "openclaw-sandbox-browser")
PUBLIC_PORT = int(os.environ["CDP_PORT"])

_WS_RE = re.compile(r'ws://127\.0\.0\.1(?::\d+)?/')


def _rewrite(data: bytes) -> bytes:
    text = data.decode("utf-8", errors="replace")
    text = _WS_RE.sub(f"ws://{PUBLIC_HOST}:{PUBLIC_PORT}/", text)
    return text.encode("utf-8")


def _pipe(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            chunk = src.recv(65536)
            if not chunk:
                break
            dst.sendall(chunk)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_WR)
            except OSError:
                pass


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        # Healthz endpoint for Zeabur / readiness probes
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.headers.get("Upgrade", "").lower() == "websocket":
            self._tunnel_ws()
        else:
            self._proxy_http()

    def _tunnel_ws(self) -> None:
        try:
            chrome = socket.create_connection((CHROME_HOST, CHROME_PORT), timeout=5)
        except Exception as exc:
            print(f"[cdp_proxy] WS tunnel: Chrome connection failed: {exc}", flush=True)
            self.send_error(503, "Chrome not reachable")
            return

        try:
            # Re-send request line + headers with Host rewritten to 127.0.0.1
            raw = f"GET {self.path} HTTP/1.1\r\n"
            for k, v in self.headers.items():
                raw += f"Host: 127.0.0.1\r\n" if k.lower() == "host" else f"{k}: {v}\r\n"
            raw += "\r\n"
            chrome.sendall(raw.encode())
        except Exception as exc:
            print(f"[cdp_proxy] WS tunnel: Failed to send request: {exc}", flush=True)
            chrome.close()
            self.send_error(502, "Failed to tunnel request")
            return

        client = self.connection
        t1 = threading.Thread(target=_pipe, args=(client, chrome), daemon=True)
        t2 = threading.Thread(target=_pipe, args=(chrome, client), daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

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
            print(f"[cdp_proxy] HTTP proxy failed for {self.path}: {exc}", flush=True)
            self.send_error(502, str(exc))

    def log_message(self, fmt, *args) -> None:  # silence access log
        pass


if __name__ == "__main__":
    print(f"[cdp_proxy] Starting on 0.0.0.0:{PUBLIC_PORT}, chrome={CHROME_HOST}:{CHROME_PORT}, public_host={PUBLIC_HOST}", flush=True)
    try:
        HTTPServer(("0.0.0.0", PUBLIC_PORT), _Handler).serve_forever()
    except Exception as exc:
        print(f"[cdp_proxy] Server error: {exc}", flush=True)
        sys.exit(1)