"""A wire tap for Python workers: wraps pyspark.worker.main and copies every byte in and out to files.

The daemon imports a custom worker module only if its name starts with "pyspark" (pyspark/daemon.py).
Laptop debugging only: it writes every row to disk, and the request carries the task's secret.
"""
import io, os
from pyspark.worker import main as real_main


class TeeIn(io.RawIOBase):
    def __init__(self, inner, log):
        self.inner, self.log = inner, log
    def readable(self):
        return True
    def readinto(self, b):
        data = self.inner.read1(len(b))      # read1: never wait for more bytes than have arrived
        b[:len(data)] = data
        self.log.write(data)
        return len(data)


class TeeOut:
    def __init__(self, inner, log):
        self.inner, self.log = inner, log
    def write(self, data):
        self.log.write(data)
        return self.inner.write(data)
    def flush(self):
        self.log.flush()
        self.inner.flush()
    def __getattr__(self, name):
        return getattr(self.inner, name)


def main(infile, outfile):
    d, pid = os.environ["TAP_DIR"], os.getpid()
    with open(os.path.join(d, f"{pid}.in"), "ab") as i, open(os.path.join(d, f"{pid}.out"), "ab") as o:
        real_main(io.BufferedReader(TeeIn(infile, i), 65536), TeeOut(outfile, o))
