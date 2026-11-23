"""Chapter 76: a pure-Python reader and writer for the VARIANT binary format, with Spark only as the referee.

Layout from common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java and
VariantBuilder.java at v4.3.0 (the Parquet VariantEncoding spec that Spark implements).
"""
import datetime, json, struct, uuid
from decimal import Decimal

PRIMITIVE, SHORT_STR, OBJECT, ARRAY = 0, 1, 2, 3
PRIMITIVES = {0: "NULL", 1: "TRUE", 2: "FALSE", 3: "INT1", 4: "INT2", 5: "INT4", 6: "INT8",
              7: "DOUBLE", 8: "DECIMAL4", 9: "DECIMAL8", 10: "DECIMAL16", 11: "DATE",
              12: "TIMESTAMP", 13: "TIMESTAMP_NTZ", 14: "FLOAT", 15: "BINARY", 16: "LONG_STR",
              20: "UUID"}
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def u(b, pos, n):
    """Little-endian unsigned integer of n bytes."""
    return int.from_bytes(b[pos:pos + n], "little")


def s(b, pos, n):
    return int.from_bytes(b[pos:pos + n], "little", signed=True)


# ---------------------------------------------------------------- metadata
def parse_metadata(m):
    """Header byte, dictionary size, offsets, keys. Returns a dict with every field and its position."""
    h = m[0]
    osz = ((h >> 6) & 0x3) + 1
    n = u(m, 1, osz)
    offs = [u(m, 1 + osz + i * osz, osz) for i in range(n + 1)]
    start = 1 + osz + (n + 1) * osz
    keys = [m[start + offs[i]:start + offs[i + 1]].decode() for i in range(n)]
    return {"version": h & 0x0F, "sorted_strings": (h >> 4) & 1, "offset_size": osz,
            "size": n, "offsets": offs, "strings_at": start, "keys": keys}


# ---------------------------------------------------------------- values
def header(b, pos):
    h = b[pos]
    return h & 0x3, h >> 2


def object_layout(v, pos):
    _, info = header(v, pos)
    large, idsz, osz = (info >> 4) & 1, ((info >> 2) & 3) + 1, (info & 3) + 1
    szb = 4 if large else 1
    n = u(v, pos + 1, szb)
    id_at = pos + 1 + szb
    off_at = id_at + n * idsz
    data_at = off_at + (n + 1) * osz
    ids = [u(v, id_at + i * idsz, idsz) for i in range(n)]
    offs = [u(v, off_at + i * osz, osz) for i in range(n + 1)]
    return {"is_large": large, "id_size": idsz, "offset_size": osz, "size": n, "id_at": id_at,
            "off_at": off_at, "data_at": data_at, "ids": ids, "offsets": offs}


def array_layout(v, pos):
    _, info = header(v, pos)
    large, osz = (info >> 2) & 1, (info & 3) + 1
    szb = 4 if large else 1
    n = u(v, pos + 1, szb)
    off_at = pos + 1 + szb
    data_at = off_at + (n + 1) * osz
    return {"is_large": large, "offset_size": osz, "size": n, "off_at": off_at, "data_at": data_at,
            "offsets": [u(v, off_at + i * osz, osz) for i in range(n + 1)]}


def type_name(v, pos=0):
    basic, info = header(v, pos)
    return {SHORT_STR: "SHORT_STR", OBJECT: "OBJECT", ARRAY: "ARRAY"}.get(basic) or PRIMITIVES.get(info, f"?{info}")


def decode(v, keys, pos=0):
    """The Python value at pos. keys is the metadata dictionary (a list of strings)."""
    basic, info = header(v, pos)
    if basic == SHORT_STR:
        return v[pos + 1:pos + 1 + info].decode()
    if basic == OBJECT:
        L = object_layout(v, pos)
        return {keys[i]: decode(v, keys, L["data_at"] + o) for i, o in zip(L["ids"], L["offsets"])}
    if basic == ARRAY:
        L = array_layout(v, pos)
        return [decode(v, keys, L["data_at"] + o) for o in L["offsets"][:-1]]
    p = pos + 1
    if info == 0: return None
    if info == 1: return True
    if info == 2: return False
    if info in (3, 4, 5, 6): return s(v, p, {3: 1, 4: 2, 5: 4, 6: 8}[info])
    if info == 7: return struct.unpack("<d", v[p:p + 8])[0]
    if info == 14: return struct.unpack("<f", v[p:p + 4])[0]
    if info in (8, 9, 10):
        scale, n = v[p], {8: 4, 9: 8, 10: 16}[info]
        return Decimal(s(v, p + 1, n)).scaleb(-scale)
    if info == 11: return datetime.date(1970, 1, 1) + datetime.timedelta(days=s(v, p, 4))
    if info == 12: return EPOCH + datetime.timedelta(microseconds=s(v, p, 8))
    if info == 13: return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=s(v, p, 8))
    if info == 15: return bytes(v[p + 4:p + 4 + u(v, p, 4)])
    if info == 16: return v[p + 4:p + 4 + u(v, p, 4)].decode()
    if info == 20: return uuid.UUID(bytes=bytes(v[p:p + 16]))
    raise ValueError(f"unknown primitive type id {info}")


# ---------------------------------------------------------------- writer (mirrors VariantBuilder)
def int_size(x):
    return 1 if x <= 0xFF else 2 if x <= 0xFFFF else 3 if x <= 0xFFFFFF else 4


class Builder:
    """Builds value and metadata from Python values the way VariantBuilder.buildJson does.

    Numbers must be given as the JSON text decides: int, Decimal (a fraction written without an
    exponent), or float (a number with an exponent, or a decimal that does not fit 38 digits).
    """
    def __init__(self, sort_ids=True):
        self.keys, self.ids, self.sort_ids = [], {}, sort_ids

    def key(self, k):
        if k not in self.ids:
            self.ids[k] = len(self.keys)
            self.keys.append(k)
        return self.ids[k]

    def value(self, x):
        if x is None: return bytes([0 << 2])
        if x is True: return bytes([1 << 2])
        if x is False: return bytes([2 << 2])
        if isinstance(x, int):
            for tid, n in ((3, 1), (4, 2), (5, 4), (6, 8)):
                if -(1 << (8 * n - 1)) <= x < (1 << (8 * n - 1)):
                    return bytes([tid << 2]) + x.to_bytes(n, "little", signed=True)
            return self.value(Decimal(x))                      # beyond a long: a decimal, scale 0
        if isinstance(x, Decimal):
            sign, digits, exp = x.as_tuple()
            scale, prec = max(0, -exp), max(len(digits), max(0, -exp))
            unscaled = int(x.scaleb(scale))
            for tid, n, maxp in ((8, 4, 9), (9, 8, 18), (10, 16, 38)):
                if scale <= maxp and prec <= maxp:
                    return bytes([tid << 2, scale]) + unscaled.to_bytes(n, "little", signed=True)
            return self.value(float(x))
        if isinstance(x, float): return bytes([7 << 2]) + struct.pack("<d", x)
        if isinstance(x, str):
            t = x.encode()
            if len(t) <= 63: return bytes([(len(t) << 2) | SHORT_STR]) + t
            return bytes([16 << 2]) + len(t).to_bytes(4, "little") + t
        if isinstance(x, list):
            parts = [self.value(e) for e in x]
            data = b"".join(parts)
            offs, o = [], 0
            for p in parts:
                offs.append(o); o += len(p)
            offs.append(o)
            large, osz = len(parts) > 255, int_size(len(data))
            hdr = bytes([((1 if large else 0) << 4 | (osz - 1) << 2) | ARRAY])
            return hdr + len(parts).to_bytes(4 if large else 1, "little") + \
                b"".join(v.to_bytes(osz, "little") for v in offs) + data
        if isinstance(x, dict):
            fields, data = [], b""
            for k, e in x.items():                             # data in input order
                fid = self.key(k)
                fields.append((k, fid, len(data)))
                data += self.value(e)
            if self.sort_ids:
                fields.sort(key=lambda f: f[0].encode("utf-16-be"))   # Java String.compareTo order
            n = len(fields)
            large = n > 255
            idsz = int_size(max([f[1] for f in fields], default=0))
            osz = int_size(len(data))
            hdr = bytes([((1 if large else 0) << 6 | (idsz - 1) << 4 | (osz - 1) << 2) | OBJECT])
            return hdr + n.to_bytes(4 if large else 1, "little") + \
                b"".join(f[1].to_bytes(idsz, "little") for f in fields) + \
                b"".join(f[2].to_bytes(osz, "little") for f in fields) + \
                len(data).to_bytes(osz, "little") + data
        raise TypeError(type(x))

    def metadata(self):
        blob = b"".join(k.encode() for k in self.keys)
        osz = int_size(max(len(blob), len(self.keys)))
        offs, o = [], 0
        for k in self.keys:
            offs.append(o); o += len(k.encode())
        offs.append(o)
        return bytes([1 | (osz - 1) << 6]) + len(self.keys).to_bytes(osz, "little") + \
            b"".join(v.to_bytes(osz, "little") for v in offs) + blob


def json_number_hook():
    """json.loads hooks that keep the distinction parse_json makes: int, plain decimal, or exponent."""
    def parse_float(t):
        return float(t) if ("e" in t or "E" in t) else Decimal(t)
    return {"parse_float": parse_float, "parse_int": int}


def encode_json(text, sort_ids=True):
    b = Builder(sort_ids)
    v = b.value(json.loads(text, **json_number_hook()))
    return v, b.metadata()


# ---------------------------------------------------------------- display
def hexdump(data, marks=(), start=0, end=None, width=16):
    end = len(data) if end is None else end
    names = {}
    for off, name in marks:
        names.setdefault(off // width * width, []).append(name)
    out = []
    for off in range(start // width * width, end, width):
        chunk = data[off:off + width]
        hx = " ".join(f"{c:02x}" for c in chunk)
        asc = "".join(chr(c) if 32 <= c < 127 else "." for c in chunk)
        note = "  <- " + "; ".join(names[off]) if off in names else ""
        out.append(f"{off:04x}  {hx:<{width * 3 - 1}}  {asc:<{width}}{note}")
    return "\n".join(out)


def spaced(b):
    return " ".join(f"{c:02x}" for c in b)
