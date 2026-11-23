"""Chapter 81: a TCP proxy that records the bytes a client sends, to see what an eavesdropper sees.

    python proxy.py LISTEN_PORT TARGET_PORT DUMP_FILE

The driver connects to the standalone master through this proxy (spark://127.0.0.1:LISTEN_PORT),
so DUMP_FILE holds the RPC messages the driver sends to the master.
"""
import socket
import sys
import threading

listen, target, dump = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
out = open(dump, "wb")
lock = threading.Lock()


def pipe(src, dst, record):
    try:
        while data := src.recv(65536):
            if record:
                with lock:
                    out.write(data)
                    out.flush()
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", listen))
srv.listen()
while True:
    c, _ = srv.accept()
    t = socket.create_connection(("127.0.0.1", target))
    threading.Thread(target=pipe, args=(c, t, True), daemon=True).start()
    threading.Thread(target=pipe, args=(t, c, False), daemon=True).start()
