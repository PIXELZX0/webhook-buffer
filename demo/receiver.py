"""시연용 목적지 서버.

- /hook 로 시작하는 경로로 들어온 요청은 웹훅으로 보고 기록한다.
- / 는 받은 웹훅 목록과 상태 전환 버튼이 있는 페이지다.
- 상태: ok(200 응답), error(500 응답), down(응답하지 않고 붙잡고 있다가 끊음 → 버퍼는 타임아웃으로 봄)
- down 또는 error 에서 ok 로 돌아오거나 서버가 시작될 때 ALIVE_URL(복구 알림 URL)을 호출한다.
  버퍼는 이 알림을 받으면 쌓아 둔 웹훅과 재시도 대기 중인 웹훅을 바로 보낸다.
"""

import html
import json
import os
import socket
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAME = os.environ.get("NAME", "receiver")
PORT = int(os.environ.get("PORT", "8080"))
ALIVE_URL = os.environ.get("ALIVE_URL", "").strip()
HANG_SECS = int(os.environ.get("HANG_SECS", "60"))

lock = threading.Lock()
received = deque(maxlen=200)
state = {"mode": "ok", "count": 0, "last_alive": "-"}


def now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def notify_alive():
    if not ALIVE_URL:
        return
    for attempt in range(5):
        try:
            req = urllib.request.Request(ALIVE_URL, method="POST", data=b"")
            with urllib.request.urlopen(req, timeout=10) as r:
                body = r.read().decode(errors="replace")
            with lock:
                state["last_alive"] = f"{now()} {body}"
            print(f"[{NAME}] alive notified: {body}", flush=True)
            return
        except Exception as e:  # noqa: BLE001
            print(f"[{NAME}] alive notify failed ({attempt + 1}/5): {e}", flush=True)
            time.sleep(2)


PAGE = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name}</title>
<style>
body{{font-family:system-ui,sans-serif;margin:0;padding:16px;background:#0f1115;color:#e6e6e6}}
h1{{margin:0 0 4px;font-size:22px}} .sub{{color:#9aa0a6;font-size:13px;margin-bottom:16px}}
.modes{{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}}
.modes button{{padding:10px 16px;border-radius:8px;border:1px solid #333;background:#1b1e24;color:#e6e6e6;font-size:15px;cursor:pointer}}
.modes button.on.ok{{background:#1f7a3a;border-color:#1f7a3a}}
.modes button.on.error{{background:#a3512a;border-color:#a3512a}}
.modes button.on.down{{background:#8a1c1c;border-color:#8a1c1c}}
.item{{border:1px solid #2a2d33;border-radius:8px;padding:8px 10px;margin-bottom:8px;background:#161920}}
.meta{{font-size:12px;color:#9aa0a6;margin-bottom:4px}} .ok-tag{{color:#4cc274}} .err-tag{{color:#e8875b}}
pre{{margin:0;white-space:pre-wrap;word-break:break-all;font-size:13px}}
</style></head><body>
<h1>{name}</h1>
<div class="sub">받은 웹훅 {count}개 · 마지막 복구 알림: {last_alive}</div>
<div class="modes">
<form method="post" action="/_mode?m=ok"><button class="ok {ok_on}">정상 (200)</button></form>
<form method="post" action="/_mode?m=error"><button class="error {error_on}">오류 (500)</button></form>
<form method="post" action="/_mode?m=down"><button class="down {down_on}">다운 (응답 없음)</button></form>
</div>
{items}
<script>setTimeout(()=>location.reload(),2000)</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="text/plain; charset=utf-8", extra=None):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _webhook(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        with lock:
            mode = state["mode"]
        if mode == "down":
            # 응답하지 않고 붙잡아 둔다. 버퍼는 타임아웃 = "응답 없음"으로 본다.
            print(f"[{NAME}] down: holding {self.command} {self.path}", flush=True)
            time.sleep(HANG_SECS)
            self.close_connection = True
            return
        text = body.decode(errors="replace")
        try:
            text = json.dumps(json.loads(text), ensure_ascii=False, indent=2)
        except Exception:  # noqa: BLE001
            pass
        code = 200 if mode == "ok" else 500
        with lock:
            state["count"] += 1
            received.appendleft(
                {
                    "time": now(),
                    "method": self.command,
                    "path": self.path,
                    "event": self.headers.get("x-demo-event", ""),
                    "code": code,
                    "body": text,
                }
            )
        print(f"[{NAME}] {self.command} {self.path} -> {code}", flush=True)
        self._send(code, "ok\n" if code == 200 else "simulated error\n")

    def _page(self):
        with lock:
            mode = state["mode"]
            items = "".join(
                f'<div class="item"><div class="meta">{r["time"]} · {html.escape(r["method"])} '
                f'{html.escape(r["path"])} · {html.escape(r["event"])} · '
                f'<span class="{"ok-tag" if r["code"] == 200 else "err-tag"}">{r["code"]}</span></div>'
                f'<pre>{html.escape(r["body"])}</pre></div>'
                for r in received
            ) or '<div class="sub">아직 받은 웹훅이 없습니다.</div>'
            page = PAGE.format(
                name=html.escape(NAME),
                count=state["count"],
                last_alive=html.escape(state["last_alive"]),
                ok_on="on" if mode == "ok" else "",
                error_on="on" if mode == "error" else "",
                down_on="on" if mode == "down" else "",
                items=items,
            )
        self._send(200, page, "text/html; charset=utf-8")

    def _set_mode(self):
        m = self.path.split("m=", 1)[-1] if "m=" in self.path else ""
        if m not in ("ok", "error", "down"):
            return self._send(400, "bad mode\n")
        with lock:
            prev, state["mode"] = state["mode"], m
        print(f"[{NAME}] mode {prev} -> {m}", flush=True)
        if prev != "ok" and m == "ok":
            threading.Thread(target=notify_alive, daemon=True).start()
        self._send(303, "", extra={"location": "/"})

    def _route(self):
        if self.path.startswith("/hook"):
            return self._webhook()
        if self.path.startswith("/_mode") and self.command == "POST":
            return self._set_mode()
        if self.path == "/_state":
            with lock:
                return self._send(200, json.dumps(state), "application/json")
        if self.command == "GET" and self.path in ("/", "/index.html"):
            return self._page()
        return self._send(404, "not found\n")

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _route


class DualStackServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


if __name__ == "__main__":
    threading.Thread(target=notify_alive, daemon=True).start()
    print(f"[{NAME}] listening on :{PORT}", flush=True)
    DualStackServer(("::", PORT), Handler).serve_forever()
