"""Chapter 30, Steps 4 and 5: the pre-shared token, what user_id means, and TLS through a proxy.

Starts the Part VIII Connect server with spark.connect.authenticate.token, then Envoy in Docker as
a TLS proxy in front of it (a self-signed certificate for localhost).

Run from code/:  uv run python ch30/auth.py
"""
import os
import re
import threading
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connect_util import server, session, close, stop, rest, error_class  # noqa: E402

HERE = Path(__file__).resolve().parent

# A self-signed certificate for localhost. gRPC reads its trusted roots once, so set the variable
# before the first TLS channel.
certs = tempfile.mkdtemp(prefix="ch30-certs-")
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                "-keyout", f"{certs}/server.key", "-out", f"{certs}/server.crt"],
               check=True, capture_output=True)
os.chmod(f"{certs}/server.key", 0o644)                     # Envoy runs as a different user
os.chmod(certs, 0o755)
os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = f"{certs}/server.crt"
TOKEN = "s3cret-for-the-book"
url = server(f"spark.connect.authenticate.token={TOKEN}")


def explain(e):
    text = str(e)
    status = re.search(r"status = (StatusCode\.\w+)", text)
    details = re.search(r'details = "([^"]*)"', text)
    if status:
        return f"{status.group(1)}: {(details.group(1) if details else '')[:60]}"
    return f"{error_class(e)}: {text.splitlines()[0][:70]}"


def attempt(label, url, wait=30, **params):
    """Open a session and run one query, in a thread, so that a client that keeps retrying cannot stop
    the script. Prints the result, the error, or that no answer came in `wait` seconds."""
    out = {}

    def run():
        try:
            s = session(url, **params)
            out["ok"] = (s.range(3).count(), s.client._user_id)
            close(s)
        except Exception as e:
            out["error"] = explain(e)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(wait)
    if "ok" in out:
        print(f"  {label:46} ok ({out['ok'][0]} rows), user_id {out['ok'][1]}")
    elif "error" in out:
        print(f"  {label:46} {out['error']}")
    else:
        print(f"  {label:46} no answer in {wait} s (the client keeps retrying)")


print("Connect server with spark.connect.authenticate.token:")
attempt("no token", url)
attempt("wrong token", url, token="guess")
attempt("right token, user_id=analyst", url, token=TOKEN, user_id="analyst")
attempt("right token, user_id=admin (any name works)", url, token=TOKEN, user_id="admin")
print("  users the server recorded:", sorted({s["userId"] for s in rest("connect/sessions")}))

attempt("right token, host 127.0.0.1 instead of localhost", url.replace("localhost", "127.0.0.1"),
        token=TOKEN)

# --- Step 5: TLS at a proxy ---------------------------------------------------------------------
envoy = subprocess.Popen(["docker", "run", "--rm", "--name", "ch30-envoy", "--network", "host",
                          "-v", f"{HERE / 'envoy.yaml'}:/etc/envoy/envoy.yaml:ro",
                          "-v", f"{certs}:/certs:ro", "envoyproxy/envoy:v1.31.5",
                          "-c", "/etc/envoy/envoy.yaml", "--log-level", "error"],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
time.sleep(4)
tls = "sc://localhost:10282"
print("\nthrough Envoy (TLS on :10282, cleartext to :10280):")
attempt("use_ssl=true, right token", tls, use_ssl="true", token=TOKEN, user_id="analyst")
attempt("use_ssl=true, no token", tls, use_ssl="true")
attempt("token without use_ssl (no TLS to localhost)", tls, token=TOKEN)
subprocess.run(["docker", "stop", "ch30-envoy"], capture_output=True)
lines = [l for l in envoy.communicate(timeout=30)[0].splitlines() if l.startswith("/")]
print(f"\nEnvoy access log: {len(lines)} RPCs, for example")
seen = set()
for l in lines:
    key = l.split()[0] + l.split()[1]
    if key not in seen:
        seen.add(key)
        print("  ", l[:100])
stop()
sys.stdout.flush()
os._exit(0)                  # a retrying client thread would keep the process alive
