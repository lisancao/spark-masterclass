"""A local, paginated tickets API for Chapter 51. Standard library only, deterministic.

GET /tickets?page=N&page_size=M  ->  {"items": [...], "page": N, "page_size": M, "total": 95}
Page 3 is flaky on purpose: every other request to it gets HTTP 503.
Every request is recorded in `log` as (page, status), so a script can count calls.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOTAL = 95
SUBJECTS = ["Where is my order?", "Refund please", "Wrong size", "Login fails", "Damaged box"]
STATUSES = ["open", "pending", "closed", "open", "closed"]
TAGS = [[], ["shipping"], ["billing", "refund"], ["account"], ["shipping", "damage"]]


def ticket(i):
    meta = {"tags": TAGS[i % 5]}
    if i % 3 == 0:
        meta["priority"] = "high" if i % 2 == 0 else "low"
    if i % 4 != 1:
        meta["customer"] = {"id": f"C{i % 17}", "tier": "gold" if i % 7 == 0 else "silver"}
    return {"id": i, "status": STATUSES[i % 5], "subject": SUBJECTS[i % 5], "meta": meta}


class TicketsAPI:
    def __init__(self):
        self.log = []
        self.flaky_hits = 0
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                q = parse_qs(urlparse(self.path).query)
                page = int(q.get("page", ["0"])[0])
                size = int(q.get("page_size", ["20"])[0])
                if page == 3:
                    api.flaky_hits += 1
                    if api.flaky_hits % 2 == 1:
                        api.log.append((page, 503))
                        self.send_response(503)
                        self.end_headers()
                        return
                api.log.append((page, 200))
                items = [ticket(i) for i in range(page * size, min((page + 1) * size, TOTAL))]
                body = json.dumps({"items": items, "page": page, "page_size": size,
                                   "total": TOTAL}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)   # port 0: the OS picks one
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/tickets"

    def take_log(self):
        """The requests since the last call, as a short summary."""
        log, self.log = self.log, []
        calls = " ".join(f"p{page}" + ("" if status == 200 else f"({status})")
                         for page, status in sorted(log, key=lambda c: (c[0], -c[1])))
        return f"{len(log)} requests: {calls}".rstrip(": ")
