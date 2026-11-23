"""Read the state files of the HDFS-backed state store with the standard library only:
lz4-java block framing, the LZ4 block format, the delta-file records and UnsafeRow fields."""
import struct


def lz4_block(src):
    """Decompress one LZ4 block (the block format, not the frame format)."""
    out, i = bytearray(), 0
    while i < len(src):
        token = src[i]
        i += 1
        n = token >> 4                      # literal length
        if n == 15:
            while True:
                b = src[i]
                i += 1
                n += b
                if b != 255:
                    break
        out += src[i:i + n]
        i += n
        if i >= len(src):                   # the last sequence has literals only
            break
        offset = src[i] | (src[i + 1] << 8)
        i += 2
        m = token & 15                      # match length minus 4
        if m == 15:
            while True:
                b = src[i]
                i += 1
                m += b
                if b != 255:
                    break
        for _ in range(m + 4):              # byte by byte: a match can overlap itself
            out.append(out[-offset])
    return bytes(out)


def lz4_java_stream(data):
    """Undo lz4-java's LZ4BlockOutputStream: 'LZ4Block', a token, two lengths, a checksum, a block."""
    out, i = bytearray(), 0
    while i < len(data):
        assert data[i:i + 8] == b"LZ4Block", data[i:i + 8]
        method = data[i + 8] & 0xF0         # 0x10 raw, 0x20 LZ4
        clen, olen, _checksum = struct.unpack_from("<iii", data, i + 9)
        body = data[i + 21:i + 21 + clen]
        out += body if method == 0x10 else lz4_block(body)
        i += 21 + clen
    return bytes(out)


def records(raw):
    """The records of a delta file: (key, value) for an update, (key, None) for a removal."""
    i = 0
    while True:
        (klen,) = struct.unpack_from(">i", raw, i)
        i += 4
        if klen == -1:
            return
        key = raw[i:i + klen]
        i += klen
        (vlen,) = struct.unpack_from(">i", raw, i)
        i += 4
        if vlen == -1:
            yield key, None
            continue
        yield key, raw[i:i + vlen]
        i += vlen


def unsafe_long(row, ordinal, nfields):
    """An 8-byte field of an UnsafeRow: after the null bitset, one 8-byte slot per field."""
    return struct.unpack_from("<q", row, ((nfields + 63) // 64) * 8 + 8 * ordinal)[0]


def unsafe_string(row, ordinal, nfields):
    """A string field: its slot holds (offset << 32 | size) into the variable-length region."""
    slot = unsafe_long(row, ordinal, nfields)
    return row[slot >> 32:(slot >> 32) + (slot & 0xFFFFFFFF)].decode()
