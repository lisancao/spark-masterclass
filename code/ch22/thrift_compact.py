"""A Thrift compact protocol reader, enough to decode a Parquet footer into {field_id: value} dicts.

Field ids and enum values are from parquet-format's parquet.thrift. Adapted from SparkTutor lesson MD45.
"""
import struct

MAGIC = b"PAR1"
TYPES = {0: "BOOLEAN", 1: "INT32", 2: "INT64", 3: "INT96", 4: "FLOAT", 5: "DOUBLE", 6: "BYTE_ARRAY",
         7: "FIXED_LEN_BYTE_ARRAY"}
CODECS = {0: "NONE", 1: "SNAPPY", 2: "GZIP", 3: "LZO", 4: "BROTLI", 5: "LZ4", 6: "ZSTD", 7: "LZ4_RAW"}
ENCODINGS = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED", 5: "DELTA_BINARY_PACKED",
             6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY", 8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}
PAGE_TYPES = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}


class Compact:
    """Decode Thrift compact protocol structs."""

    def __init__(self, buf, pos=0):
        self.buf, self.pos = buf, pos

    def byte(self):
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self):
        shift = result = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, t):
        if t in (1, 2):                      # booleans are in the field header
            return t == 1
        if t == 3:                           # i8
            return struct.unpack("b", bytes([self.byte()]))[0]
        if t in (4, 5, 6):                   # i16, i32, i64: zigzag varints
            return self.zigzag()
        if t == 7:                           # double: 8 bytes, little-endian
            v = struct.unpack_from("<d", self.buf, self.pos)[0]
            self.pos += 8
            return v
        if t == 8:                           # binary: varint length, then the bytes
            n = self.varint()
            v = bytes(self.buf[self.pos:self.pos + n])
            self.pos += n
            return v
        if t in (9, 10):                     # list or set: size and element type in one byte
            h = self.byte()
            size, et = h >> 4, h & 0x0F
            if size == 15:
                size = self.varint()
            if et in (1, 2):
                return [self.byte() == 1 for _ in range(size)]
            return [self.value(et) for _ in range(size)]
        if t == 11:                          # map
            size = self.varint()
            if size == 0:
                return {}
            kv = self.byte()
            return {self.value(kv >> 4): self.value(kv & 0x0F) for _ in range(size)}
        if t == 12:
            return self.struct()
        raise ValueError(f"unknown compact type {t} at byte {self.pos}")

    def struct(self):
        fields, last = {}, 0
        while True:
            h = self.byte()
            if h == 0:                       # STOP
                return fields
            delta, t = h >> 4, h & 0x0F
            fid = last + delta if delta else self.zigzag()
            fields[fid] = self.value(t)
            last = fid

    @staticmethod
    def footer_len(path):
        with open(path, "rb") as f:
            f.seek(-8, 2)
            return struct.unpack("<I", f.read(4))[0]


def footer_bytes(path):
    """The Thrift FileMetaData bytes: PAR1, column chunks, footer, 4-byte length, PAR1."""
    with open(path, "rb") as f:
        data = f.read()
    if len(data) < 12 or data[:4] != MAGIC or data[-4:] != MAGIC:
        raise ValueError(f"{path} is not a Parquet file (no PAR1 magic)")
    (length,) = struct.unpack("<I", data[-8:-4])
    return data[-8 - length:-8]


def page_headers(path, chunk_md):
    """Walk the pages of one column chunk: (page type, encoding, values, compressed bytes)."""
    with open(path, "rb") as f:
        data = f.read()
    start = chunk_md.get(11) or chunk_md[9]                 # dictionary page first, if there is one
    end = start + chunk_md[7]
    pos, out = start, []
    while pos < end:
        c = Compact(data, pos)
        h = c.struct()
        ptype = PAGE_TYPES[h[1]]
        info = h.get(5) or h.get(7) or h.get(8) or {}       # data, dictionary or data v2 page header
        enc = info.get(4) if ptype == "DATA_PAGE_V2" else info.get(2)
        out.append((ptype, ENCODINGS.get(enc), info.get(1), h[3]))
        pos = c.pos + h[3]
    return out
