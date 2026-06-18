#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# ftdc_usage.py
#       Read WiredTiger per-btree "usage" statistics out of mongod FTDC.
#
# We decode FTDC ourselves rather than via pyftdc, for two reasons:
#   1. The usage detail slots embed the btree identity in the metric *name*
#      (usage_(id=N)_<uri>) and the hot set moves over the run, so FTDC stores a
#      different metric schema per chunk. pyftdc collapses the file to one schema
#      and loses every identity but the first chunk's. Decoding each chunk under
#      its own reference recovers them all.
#   2. The usage counters reset every dhandle-sweep, so their FTDC series are
#      non-monotonic. pyftdc's delta reconstruction desyncs on such series and
#      yields impossible (negative) values; reconstructing in wrap-around uint64
#      is exact.
#
# FTDC layout recap: a metrics file is a sequence of BSON documents; a metric
# chunk (type 1) carries a zlib blob = [reference sample BSON][uint32 metric
# count][uint32 delta-sample count][delta stream]. The reference defines the
# metric set/order and sample 0; deltas are varint-encoded, metric-major, with
# run-length-encoded zero runs. Metric enumeration must match mongod exactly --
# bool/int32/int64/double/date count as one slot, BSON timestamp as two -- so we
# walk the raw BSON by type byte (pymongo coerces int widths and miscounts).

import math
import re
import struct
import zlib

from . import bson_simple

WT_PREFIX = "serverStatus.wiredTiger."
_MASK = (1 << 64) - 1

# Replica-set role bools (used to label primary vs secondary when several FTDC
# streams are loaded together). These are not usage stats but ride in the same FTDC.
REPL_PRIMARY = "serverStatus.repl.isWritablePrimary"
REPL_SECONDARY = "serverStatus.repl.secondary"

# Display ordering for the seven sampled operations, and the description phrase
# (as it appears in the FTDC metric name) mapping onto each. Builds without
# insert-overwrite simply never emit that column; it stays zero.
OPS = ["search", "search_near", "insert", "insert_overwrite", "update", "modify", "remove"]
_OP_BY_PHRASE = {
    "searches": "search",
    "search-near calls": "search_near",
    "inserts": "insert",
    "insert-overwrites": "insert_overwrite",
    "updates": "update",
    "modifies": "modify",
    "removes": "remove",
}
READ_OPS = ("search", "search_near")

POSITIONS = ["left", "near_left", "middle", "near_right", "right"]
_POS_BY_PHRASE = {
    "on the leftmost leaf": "left",
    "near the leftmost leaf": "near_left",
    "on a middle leaf": "middle",
    "near the rightmost leaf": "near_right",
    "on the rightmost leaf": "right",
}

LEVELS = ["leaf", "i1", "i2", "i3"]
_LEVEL_BY_PHRASE = {
    "at the leaf": "leaf",
    "one level above the leaf": "i1",
    "two levels above the leaf": "i2",
    "three or more levels above the leaf": "i3",
}
_COMP_IDX = {"observation count": 0, "byte sum": 1, "sum of squares": 2}

_OP_RE = re.compile(r"^number of sampled (.+) (on the \w+ leaf|near the \w+ leaf|on a middle leaf)$")
_KEY_RE = re.compile(r"^sampled key-size (byte sum|observation count|sum of squares) (.+)$")
_VAL_RE = re.compile(r"^sampled value-size (byte sum|observation count|sum of squares)$")
_SPLIT = {
    "estimated number of leaf splits": "count",
    "number of pages resulting from sampled leaf splits": "pages",
    "estimated total keys across sampled splitting pages": "keys",
}


def parse_field(desc):
    """Classify a usage field description: ('op',pos,op) / ('key',level,compidx) /
    ('val',compidx) / ('split',which) / ('streak',), or None."""
    if desc == "consecutive intervals in top set":
        return ("streak",)
    if desc.startswith("btree type"):
        return ("type",)
    if desc in _SPLIT:
        return ("split", _SPLIT[desc])
    m = _VAL_RE.match(desc)
    if m:
        return ("val", _COMP_IDX[m.group(1)])
    m = _KEY_RE.match(desc)
    if m and m.group(2) in _LEVEL_BY_PHRASE:
        return ("key", _LEVEL_BY_PHRASE[m.group(2)], _COMP_IDX[m.group(1)])
    m = _OP_RE.match(desc)
    if m:
        op = _OP_BY_PHRASE.get(m.group(1))
        pos = _POS_BY_PHRASE.get(m.group(2))
        if op and pos:
            return ("op", pos, op)
    return None


def _mean_sd(n, s, sq):
    if n <= 0:
        return (None, None, 0)
    mean = s / n
    var = sq / n - mean * mean
    return (mean, math.sqrt(var) if var > 0 else 0.0, n)


class Btree:
    """One btree's usage within one interval (or an aggregate of intervals)."""

    SMALL_N = 3

    def __init__(self, bid, uri):
        self.id = bid
        self.uri = uri
        self.ops = {(p, o): 0 for p in POSITIONS for o in OPS}
        self.key = {lvl: [0, 0, 0] for lvl in LEVELS}  # [n, sum, sumsq]
        self.val = [0, 0, 0]
        self.split = {"count": 0, "pages": 0, "keys": 0}
        self.streak = 0
        self.type = 0  # WT_BTREE_TYPE: 1 col-fix, 2 col-var, 3 row; 0 unknown

    def is_column_store(self):
        return self.type in (1, 2)

    def add(self, other):
        for k in self.ops:
            self.ops[k] += other.ops[k]
        for lvl in LEVELS:
            for i in range(3):
                self.key[lvl][i] += other.key[lvl][i]
        for i in range(3):
            self.val[i] += other.val[i]
        for k in self.split:
            self.split[k] += other.split[k]
        self.streak = max(self.streak, other.streak)
        if other.type:
            self.type = other.type

    def op_total(self):
        return sum(self.ops.values())

    access_total = op_total

    def read_frac(self):
        tot = self.op_total()
        if not tot:
            return None
        return sum(self.ops[(p, o)] for p in POSITIONS for o in READ_OPS) / tot

    def position_weights(self):
        return [sum(self.ops[(p, o)] for o in OPS) for p in POSITIONS]

    def op_by_position(self, op):
        return [self.ops[(p, op)] for p in POSITIONS]

    def op_total_for(self, op):
        return sum(self.ops[(p, op)] for p in POSITIONS)

    def key_stats(self, level):
        return _mean_sd(*self.key[level])

    def val_stats(self):
        return _mean_sd(*self.val)


class Interval:
    def __init__(self, ts_ms):
        self.ts_ms = ts_ms
        self.btrees = {}     # id -> Btree
        self.ranks = []      # [(btree_id, access_total)] sorted by rank
        self.active_count = 0  # connection-level: btrees with any sampled activity
        self.version = 0       # FTDC usage schema version (0 = predates the version stat)
        self.is_primary = False
        self.is_secondary = False


class Timeline:
    def __init__(self):
        self.intervals = []

    def span_ms(self):
        if not self.intervals:
            return (0, 0)
        return (self.intervals[0].ts_ms, self.intervals[-1].ts_ms)


# --- raw FTDC decode --------------------------------------------------------

def _walk_metrics(raw, start, prefix, out):
    """Append (name, base_value) for each FTDC metric slot in the BSON document at
    raw[start], in document order. Mirrors mongod's metric enumeration: bool /
    int32 / int64 / double / datetime each take one slot, a BSON timestamp two
    (increment, seconds); strings, ObjectId, null and decimal are not metrics."""
    bs = bson_simple
    for name, t, value in bs.iter_document(raw, start):
        nm = prefix + name.decode("utf-8", "replace")
        if t == bs.DOC or t == bs.ARRAY:
            _walk_metrics(raw, value, nm + ".", out)
        elif t == bs.BOOL:
            out.append((nm, int(value)))
        elif t == bs.INT32 or t == bs.INT64 or t == bs.DATETIME:
            out.append((nm, value))
        elif t == bs.DOUBLE:
            out.append((nm, int(value)))
        elif t == bs.TIMESTAMP:
            inc, sec = value
            out.append((nm, inc))
            out.append((nm + "\x00ts_sec", sec))


def _read_varint(raw, p):
    r = 0
    s = 0
    while True:
        b = raw[p]
        p += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80):
            return r, p
        s += 7


def _decode_chunk(blob):
    """Decode one metric chunk. Returns (names, samples) where names is the list
    of usage metric names plus 'start', and samples is a list of dicts mapping
    those names to per-sample values (sample 0 = reference, then one per delta)."""
    raw = zlib.decompress(blob[4:])
    reflen = struct.unpack_from("<i", raw, 0)[0]
    if b"usage_(id=" not in raw[0:reflen]:
        return None
    metrics = []
    _walk_metrics(raw, 0, "", metrics)
    p = reflen
    nmet = struct.unpack_from("<I", raw, p)[0]
    nsamp = struct.unpack_from("<I", raw, p + 4)[0]
    p += 8
    if nmet != len(metrics):
        raise ValueError(f"metric count mismatch: header {nmet} != walked {len(metrics)}")

    # Indices we actually reconstruct: usage_* metrics and the per-sample clock.
    wanted = {}
    for k, (name, base) in enumerate(metrics):
        if name.startswith(WT_PREFIX + "usage_") or name == "start" or \
                name in (REPL_PRIMARY, REPL_SECONDARY):
            wanted[k] = (name, base)

    # Walk the metric-major delta stream once; keep only wanted metrics' deltas.
    # Each wanted metric k reconstructs across nsamp deltas at global indices
    # [k*nsamp, (k+1)*nsamp). Zero runs (the bulk) are skipped by index advance.
    deltas = {k: [0] * nsamp for k in wanted}
    total = nmet * nsamp
    gidx = 0
    while gidx < total:
        v, p = _read_varint(raw, p)
        if v == 0:
            cnt, p = _read_varint(raw, p)
            gidx += cnt + 1
        else:
            k = gidx // nsamp
            if k in deltas:
                deltas[k][gidx % nsamp] = v
            gidx += 1

    names = [wanted[k][0] for k in wanted]
    samples = []
    cur = {wanted[k][0]: wanted[k][1] & _MASK for k in wanted}
    samples.append(dict(cur))
    for j in range(nsamp):
        for k in wanted:
            d = deltas[k][j]
            if d:
                nm = wanted[k][0]
                cur[nm] = (cur[nm] + d) & _MASK
        samples.append(dict(cur))
    return names, samples


_SLOT_RE = re.compile(r"^usage_\(id=(\d+)\)_(.+)$")


def _apply_field(bt, field, val):
    kind = parse_field(field)
    if kind is None:
        return
    if kind[0] == "op":
        bt.ops[(kind[1], kind[2])] = val
    elif kind[0] == "key":
        bt.key[kind[1]][kind[2]] = val
    elif kind[0] == "val":
        bt.val[kind[1]] = val
    elif kind[0] == "split":
        bt.split[kind[1]] = val
    elif kind[0] == "streak":
        bt.streak = val
    elif kind[0] == "type":
        bt.type = val


def _sample_to_interval(sample):
    """Build an Interval from one decoded sample (name->value dict)."""
    ts = sample.get("start", 0)
    iv = Interval(ts)
    ranks = {}
    for name, val in sample.items():
        if not name.startswith(WT_PREFIX + "usage_"):
            continue
        slot, _, field = name[len(WT_PREFIX):].partition(".")
        if slot == "usage_active_btrees":
            iv.active_count = val
            continue
        if slot == "usage_version":
            iv.version = val
            continue
        if slot.startswith("usage_rank_"):
            idx = int(slot.rsplit("_", 1)[1])
            rid, rax = ranks.get(idx, (0, 0))
            if field == "btree id":
                rid = val
            elif field == "access total":
                rax = val
            ranks[idx] = (rid, rax)
            continue
        m = _SLOT_RE.match(slot)
        if m:
            bid, uri = int(m.group(1)), m.group(2)
        elif slot == "usage_hs":
            bid, uri = -1, "WiredTigerHS"
        elif slot == "usage_sample":
            bid, uri = -2, "random"
        else:
            continue  # usage_unused_* etc.
        bt = iv.btrees.get(bid)
        if bt is None:
            bt = iv.btrees[bid] = Btree(bid, uri)
        _apply_field(bt, field, val)
    iv.ranks = [ranks[k] for k in sorted(ranks) if ranks[k][0]]
    iv.is_primary = bool(sample.get(REPL_PRIMARY, 0))
    iv.is_secondary = bool(sample.get(REPL_SECONDARY, 0))
    return iv


def _interval_sig(iv):
    return (
        tuple(sorted((b.id, b.op_total()) for b in iv.btrees.values())),
        tuple(iv.ranks),
    )


def load(path):
    """Load a Timeline of distinct usage snapshots from an FTDC metrics file."""
    buf = open(path, "rb").read()
    n = len(buf)
    tl = Timeline()
    prev_sig = None
    off = 0
    while off < n:
        total = struct.unpack_from("<i", buf, off)[0]
        if total <= 0 or off + total > n:
            break
        typ = data = None
        for name, t, value in bson_simple.iter_document(buf, off):
            if name == b"type" and t in (bson_simple.INT32, bson_simple.INT64, bson_simple.BOOL):
                typ = int(value)
            elif name == b"data" and t == bson_simple.BINARY:
                data = value
        off += total
        if typ != 1 or data is None:
            continue  # type 0 is metadata; only metric chunks carry usage data
        decoded = _decode_chunk(data)
        if decoded is None:
            continue
        _, samples = decoded
        for sample in samples:
            iv = _sample_to_interval(sample)
            sig = _interval_sig(iv)
            if sig != prev_sig:
                tl.intervals.append(iv)
                prev_sig = sig
    tl.intervals.sort(key=lambda iv: iv.ts_ms)
    return tl


def merge_timelines(timelines):
    """Combine several timelines (e.g. successive FTDC files from one node, each
    covering a different timespan) into one, ordered by time."""
    out = Timeline()
    for tl in timelines:
        out.intervals.extend(tl.intervals)
    out.intervals.sort(key=lambda iv: iv.ts_ms)
    return out


def timeline_role(tl):
    """The replica-set role this stream spent the most time in: 'primary',
    'secondary', or 'unknown'. Role can change over a run (failover); this is the
    dominant role across all intervals."""
    prim = sum(1 for iv in tl.intervals if iv.is_primary)
    sec = sum(1 for iv in tl.intervals if iv.is_secondary)
    if prim == 0 and sec == 0:
        return "unknown"
    return "primary" if prim >= sec else "secondary"
