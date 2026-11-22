"""A TCP proxy that can cut every open connection once, for fault injection on Spark Connect.

    proxy = FlakyProxy(listen=10281, target=10280).start()
    proxy.cut_after(4_000_000)     # cut once, after 4 MB more from the server to the client

The proxy forwards bytes in both directions on a background thread. gRPC runs one HTTP/2
connection per channel, so cutting the TCP connection breaks every stream on it, which is what a
load balancer restart, a NAT timeout or a flaky Wi-Fi link does to a client.
"""
import socket
import threading


class FlakyProxy:
    def __init__(self, listen, target, host="127.0.0.1"):
        self.listen, self.target, self.host = listen, target, host
        self._budget = None              # bytes left before the cut, or None when not armed
        self._lock = threading.Lock()
        self._conns = []
        self.cuts = 0

    def start(self):
        self._srv = socket.create_server((self.host, self.listen), reuse_port=True)
        threading.Thread(target=self._accept, daemon=True).start()
        return self

    def cut_after(self, nbytes):
        with self._lock:
            self._budget = nbytes

    def _accept(self):
        while True:
            client, _ = self._srv.accept()
            server = socket.create_connection((self.host, self.target))
            with self._lock:
                self._conns.append((client, server))
            threading.Thread(target=self._pump, args=(client, server, False), daemon=True).start()
            threading.Thread(target=self._pump, args=(server, client, True), daemon=True).start()

    def _pump(self, src, dst, downstream):
        try:
            while data := src.recv(65536):
                dst.sendall(data)
                if downstream and self._count(len(data)):
                    self._cut_all()
                    return
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def _count(self, n):
        with self._lock:
            if self._budget is None:
                return False
            self._budget -= n
            if self._budget > 0:
                return False
            self._budget = None
            return True

    def _cut_all(self):
        with self._lock:
            conns, self._conns = self._conns, []
            self.cuts += 1
        for pair in conns:
            for s in pair:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
                except OSError:
                    pass
