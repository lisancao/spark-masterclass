"""Chapter 30 helpers: start and stop the Part VIII Connect server with chapter settings, open
sessions, and read the REST API of the server."""
import json
import os
import subprocess
import urllib.request
from pathlib import Path

from pyspark.sql.connect.session import SparkSession

CODE = Path(__file__).resolve().parents[1]
PORT = int(os.environ.get("CONNECT_PORT", "10280"))
UI = int(os.environ.get("CONNECT_UI_PORT", "4128"))


def server(*confs, port=PORT, ui=UI, env=None):
    """(Re)start a Connect server with --conf k=v for each conf. Returns sc://localhost:port."""
    args = [str(CODE / "ch72" / "server.sh"), "start"]
    for c in confs:
        args += ["--conf", c]
    e = dict(os.environ, CONNECT_PORT=str(port), CONNECT_UI_PORT=str(ui), **(env or {}))
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, env=e)
    return f"sc://localhost:{port}"


def stop(port=PORT, ui=UI):
    e = dict(os.environ, CONNECT_PORT=str(port), CONNECT_UI_PORT=str(ui))
    subprocess.run([str(CODE / "ch72" / "server.sh"), "stop"], check=True, env=e)


def session(url, **params):
    """A new Connect session; params go into the connection string (user_id, token, ...)."""
    extra = "".join(f";{k}={v}" for k, v in params.items())
    return SparkSession.builder.remote(url + ("/" + extra if extra else "")).create()


def close(*sessions):
    for s in sessions:
        try:
            s.client.release_session()
        except Exception:
            pass
        s.client.close()


def rest(path, ui=UI):
    base = f"http://localhost:{ui}/api/v1"
    with urllib.request.urlopen(f"{base}/applications") as r:
        app = json.load(r)[0]["id"]
    with urllib.request.urlopen(f"{base}/applications/{app}/{path}") as r:
        return json.load(r)


def error_class(e):
    """The error class of a Spark error, or the exception type."""
    cond = getattr(e, "getCondition", lambda: None)() or getattr(e, "getErrorClass", lambda: None)()
    return cond or type(e).__name__
