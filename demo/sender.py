"""시연용 웹훅 발신 서버.

TARGET_URL 로 INTERVAL_SECS 마다 가짜 결제/주문 이벤트를 보낸다.
/ 페이지에서 보낸 기록을 보고, 바로 보내기·일시정지를 할 수 있다.
"""

import html
import json
import os
import random
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
TARGET_URL = os.environ.get("TARGET_URL", "").strip()
INTERVAL = float(os.environ.get("INTERVAL_SECS", "10"))

EVENTS = ["order.created", "payment.succeeded", "payment.failed", "refund.created", "customer.updated"]

lock = threading.Lock()
sent = deque(maxlen=200)
state = {"paused": False, "seq": 0}


def now():
    return datetime.now(timezone.utc)


def send_one():
    with lock:
        state["seq"] += 1
        seq = state["seq"]
    event = random.choice(EVENTS)
    payload = {
        "id": f"evt_{seq:05d}",
        "type": event,
        "amount": random.randint(1, 500) * 100,
        "currency": "KRW",
        "created_at": now().isoformat(timespec="seconds"),
    }
    data = json.dumps(payload).encode()
    path = "/" + event.split(".")[0]
    url = TARGET_URL.rstrip("/") + path
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"content-type": "application/json", "x-demo-event": event},
    )
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            result = str(r.status)
    except urllib.error.HTTPError as e:
        result = str(e.code)
    except Exception as e:  # noqa: BLE001
        result = f"error: {e}"
    ms = int((time.monotonic() - t0) * 1000)
    with lock:
        sent.appendleft({"time": now().strftime("%H:%M:%S"), "id": payload["id"], "event": event, "path": path, "result": result, "ms": ms})
    print(f"sent {payload['id']} {event} -> {result} ({ms}ms)", flush=True)


def loop():
    while True:
        time.sleep(INTERVAL)
        with lock:
            paused = state["paused"]
        if not paused and TARGET_URL:
            send_one()


PAGE = """<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>webhook sender</title>
<style>
body{{font-family:system-ui,sans-serif;margin:0;padding:16px;background:#0f1115;color:#e6e6e6}}
h1{{margin:0 0 4px;font-size:22px}} .sub{{color:#9aa0a6;font-size:13px;margin-bottom:16px;word-break:break-all}}
.bar{{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}}
.bar button{{padding:10px 16px;border-radius:8px;border:1px solid #333;background:#1b1e24;color:#e6e6e6;font-size:15px;cursor:pointer}}
.bar button.primary{{background:#2b5fd9;border-color:#2b5fd9}}
table{{width:100%;border-collapse:collapse;font-size:14px}} td,th{{text-align:left;padding:6px 8px;border-bottom:1px solid #2a2d33}}
th{{color:#9aa0a6;font-weight:500}} .good{{color:#4cc274}} .bad{{color:#e8875b}}
</style></head><body>
<h1>Webhook sender</h1>
<div class="sub">대상: {target} · {interval}초마다 · {status}</div>
<div class="bar">
<form method="post" action="/_send"><button class="primary">지금 1개 보내기</button></form>
<form method="post" action="/_burst"><button>10개 한꺼번에</button></form>
<form method="post" action="/_toggle"><button>{toggle}</button></form>
</div>
<table><tr><th>시각</th><th>ID</th><th>이벤트</th><th>경로</th><th>응답</th></tr>{rows}</table>
<script>setTimeout(()=>location.reload(),2000)</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="text/plain; charset=utf-8", extra=None):
        data = body.encode()
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _back(self):
        self._send(303, "", extra={"location": "/"})

    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            return self._send(404, "not found\n")
        with lock:
            rows = "".join(
                f'<tr><td>{r["time"]}</td><td>{r["id"]}</td><td>{html.escape(r["event"])}</td>'
                f'<td>{html.escape(r["path"])}</td><td class="{"good" if r["result"] == "202" else "bad"}">'
                f'{html.escape(r["result"])} · {r["ms"]}ms</td></tr>'
                for r in sent
            )
            paused = state["paused"]
        page = PAGE.format(
            target=html.escape(TARGET_URL or "(TARGET_URL 미설정)"),
            interval=f"{INTERVAL:g}",
            status="일시정지됨" if paused else "보내는 중",
            toggle="다시 시작" if paused else "일시정지",
            rows=rows,
        )
        self._send(200, page, "text/html; charset=utf-8")

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        if self.path == "/_send":
            threading.Thread(target=send_one, daemon=True).start()
        elif self.path == "/_burst":
            for _ in range(10):
                threading.Thread(target=send_one, daemon=True).start()
        elif self.path == "/_toggle":
            with lock:
                state["paused"] = not state["paused"]
        else:
            return self._send(404, "not found\n")
        self._back()


class DualStackServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True

    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    print(f"sender listening on :{PORT}, target={TARGET_URL}, every {INTERVAL}s", flush=True)
    DualStackServer(("::", PORT), Handler).serve_forever()
