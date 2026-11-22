"""Chapter 40: a misbehaving classification service and a small client, for the inference scripts.

Copied from the SparkTutor lesson "Model Inference in Production" (the parts the lesson gives).
The service runs in a thread of the driver on 127.0.0.1; local-mode Python workers call it over HTTP.
"""
import hashlib
import http.client
import json
import random
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

MODEL_VERSION = "kw-1"
TIMEOUT_SECONDS = 0.5
class FakeClassifier:
    """A keyword "model" behind an HTTP API, with the failures real services have.

    POST /v1/classify with {"model": version, "items": [{"key": ..., "text": ...}]}
    and an Idempotency-Key header. Returns {"results": [...]}, one per item:
    {"key", "label", "model_version"} or {"key", "error", "retryable"}.

    - more than 16 requests in any second (all clients together): HTTP 429 with
      a Retry-After header
    - more than 20 items in one request: HTTP 413
    - an item whose text has no letters: a permanent error ("no text")
    - about one key in seven: "model busy" the first time it is seen (retryable)
    - about one request in five: the answer is ready but sent only after 1 second,
      longer than a sensible client timeout; a retry with the same
      Idempotency-Key gets the stored answer without running the model again
    """

    RULES = [("warranty", ["warranty", "broken", "repair", "defective", "crack", "batter", "stopped"]),
             ("returns", ["return", "refund", "exchange", "changed my mind", "send it back"]),
             ("shipping", ["ship", "deliver", "parcel", "track", "arrive", "express", "courier"]),
             ("privacy", ["privacy", "data", "unsubscribe", "delete", "account", "marketing"])]

    def __init__(self, version=MODEL_VERSION):
        self.version = version
        self.lock = threading.Lock()
        self.model_runs = Counter()        # key -> times the model actually ran on it
        self.items_received = Counter()    # key -> times it arrived in a request
        self.status = Counter()            # HTTP status codes sent
        self.requests = 0
        self.max_items = 0
        self.seen = set()
        self.slow_once = set()
        self.answers = {}                  # Idempotency-Key -> stored response body
        self.window = []                   # arrival times, for the rate limit

    def label(self, text):
        t = (text or "").lower()
        for label, words in self.RULES:
            if any(w in t for w in words):
                return label
        return "other"

    def handle(self, body, idem_key):
        """Returns (status, headers, payload, delay_seconds)."""
        now = time.monotonic()
        with self.lock:
            self.requests += 1
            self.window = [t for t in self.window if now - t < 1.0] + [now]
            if len(self.window) > 16:
                self.status[429] += 1
                return 429, {"Retry-After": "0.5"}, {"error": "rate limited"}, 0
            items = body.get("items", [])
            self.max_items = max(self.max_items, len(items))
            if len(items) > 20:
                self.status[413] += 1
                return 413, {}, {"error": "too many items"}, 0
            if idem_key and idem_key in self.answers:
                self.status[200] += 1
                return 200, {}, self.answers[idem_key], 0
            results = []
            for item in items:
                key, text = item["key"], item.get("text")
                self.items_received[key] += 1
                if not any(ch.isalpha() for ch in (text or "")):
                    results.append({"key": key, "error": "no text", "retryable": False})
                elif int(key[:8], 16) % 7 == 0 and key not in self.seen:
                    self.seen.add(key)
                    results.append({"key": key, "error": "model busy", "retryable": True})
                else:
                    self.seen.add(key)
                    self.model_runs[key] += 1
                    results.append({"key": key, "label": self.label(text), "model_version": self.version})
            payload = {"results": results}
            if idem_key:
                self.answers[idem_key] = payload
            delay = 0
            if idem_key and int(hashlib.sha256(idem_key.encode()).hexdigest()[:8], 16) % 5 == 0 \
                    and idem_key not in self.slow_once:
                self.slow_once.add(idem_key)
                delay = 1.0
            self.status[200] += 1
            return 200, {}, payload, delay

    def start(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                status, headers, payload, delay = service.handle(body, self.headers.get("Idempotency-Key"))
                if delay:
                    time.sleep(delay)
                data = json.dumps(payload).encode()
                try:
                    self.send_response(status)
                    for k, v in headers.items():
                        self.send_header(k, v)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass                     # the client gave up waiting

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def stop(self):
        self.server.shutdown()
        self.server.server_close()



class RetryableError(Exception):
    """The whole request failed in a way that may heal (429, 5xx, timeout, reset)."""
    def __init__(self, reason, retry_after=None):
        super().__init__(reason)
        self.retry_after = retry_after


class PermanentError(Exception):
    """The whole request was refused and retrying will not help (4xx other than 429)."""


class ServiceClient:
    """One keep-alive connection to the service. Create one per partition, never per row."""

    def __init__(self, url, timeout=TIMEOUT_SECONDS):
        u = urlparse(url)
        self.host, self.port, self.timeout = u.hostname, u.port, timeout
        self.conn = None

    def classify(self, items, model, idempotency_key):
        """POST the items; return the per-item result dicts, or raise one of the errors above."""
        body = json.dumps({"model": model, "items": items})
        try:
            if self.conn is None:
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self.conn.request("POST", "/v1/classify", body=body,
                              headers={"Content-Type": "application/json",
                                       "Idempotency-Key": idempotency_key})
            resp = self.conn.getresponse()
            data = resp.read()
        except (TimeoutError, ConnectionError, http.client.HTTPException, OSError) as e:
            self.close()                     # a timed-out connection cannot be reused
            raise RetryableError(f"{type(e).__name__}: {e}")
        if resp.status == 429 or resp.status >= 500:
            ra = resp.getheader("Retry-After")
            raise RetryableError(f"HTTP {resp.status}", float(ra) if ra else None)
        if resp.status != 200:
            raise PermanentError(f"HTTP {resp.status}: {data[:200]!r}")
        return json.loads(data)["results"]

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class RateLimiter:
    """At most `rate` calls per second from this task (a token bucket of size 1)."""

    def __init__(self, rate):
        self.interval = 1.0 / rate
        self.next_time = time.monotonic()

    def wait(self):
        now = time.monotonic()
        if now < self.next_time:
            time.sleep(self.next_time - now)
        self.next_time = max(now, self.next_time) + self.interval


def backoff_seconds(attempt, base=0.05, cap=2.0):
    """Exponential backoff with full jitter: a random wait in [0, base * 2**attempt], capped."""
    return random.uniform(0, min(cap, base * 2 ** attempt))


def idempotency_key(model, keys):
    """Same model and same items: same key, so a retry is recognized as a retry."""
    return hashlib.sha256((model + "|" + ",".join(sorted(keys))).encode()).hexdigest()


