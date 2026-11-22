"""Chapter 44, Steps 4 and 5: the pre-shared token, what user_id means, and TLS through a proxy.

Starts the Part VIII Connect server with spark.connect.authenticate.token, then Envoy in Docker as
a TLS proxy in front of it (a self-signed certificate for localhost).

Run from code/:  uv run python ch44/auth.py
"""
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connect_util import server, session, close, stop, rest, error_class  # noqa: E402

HERE = Path(__file__).resolve().parent
TOKEN = "s3cret-for-the-book"
url = server(f"spark.connect.authenticate.token={TOKEN}")


def attempt(label, url, **params):
    try:
        s = session(url, **params)
        n = s.range(3).count()
        user = s.client._user_id
        close(s)
        print(f"  {label:46} ok ({n} rows), user_id {user}")
    except Exception as e:
        msg = str(e).splitlines()[0][:70]
        print(f"  {label:46} {error_class(e)}: {msg}")


print("Connect server with spark.connect.authenticate.token:")
attempt("no token", url)
attempt("wrong token", url, token="guess")
attempt("right token, user_id=analyst", url, token=TOKEN, user_id="analyst")
attempt("right token, user_id=admin (any name works)", url, token=TOKEN, user_id="admin")
print("  users the server recorded:", sorted({s["userId"] for s in rest("connect/sessions")}))

attempt("right token, host 127.0.0.1 instead of localhost", url.replace("localhost", "127.0.0.1"),
        token=TOKEN)

# --- Step 5: TLS at a proxy ---------------------------------------------------------------------
certs = tempfile.mkdtemp(prefix="ch44-certs-")
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                "-keyout", f"{certs}/server.key", "-out", f"{certs}/server.crt"],
               check=True, capture_output=True)
os.chmod(f"{certs}/server.key", 0o644)                     # Envoy runs as a different user
os.chmod(certs, 0o755)
envoy = subprocess.Popen(["docker", "run", "--rm", "--name", "ch44-envoy", "--network", "host",
                          "-v", f"{HERE / 'envoy.yaml'}:/etc/envoy/envoy.yaml:ro",
                          "-v", f"{certs}:/certs:ro", "envoyproxy/envoy:v1.31.5",
                          "-c", "/etc/envoy/envoy.yaml", "--log-level", "error"],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
time.sleep(4)
os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = f"{certs}/server.crt"   # trust the test CA
tls = "sc://localhost:10282"
print("\nthrough Envoy (TLS on :10282, cleartext to :10280):")
attempt("use_ssl=true, right token", tls, use_ssl="true", token=TOKEN, user_id="analyst")
attempt("use_ssl=true, no token", tls, use_ssl="true")
attempt("token without use_ssl (no TLS to localhost)", tls, token=TOKEN)
subprocess.run(["docker", "stop", "ch44-envoy"], capture_output=True)
lines = [l for l in envoy.communicate(timeout=30)[0].splitlines() if l.startswith("/")]
print(f"\nEnvoy access log: {len(lines)} RPCs, for example")
seen = set()
for l in lines:
    key = l.split()[0] + l.split()[1]
    if key not in seen:
        seen.add(key)
        print("  ", l[:100])
stop()
