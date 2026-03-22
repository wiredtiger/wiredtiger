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

import json
from typing import Any, TextIO

from wt_decode.core import btree


def page_to_dict(page: btree.WTPage) -> dict[str, Any]:
    """Convert a decoded WTPage to a JSON-serializable dictionary."""
    result: dict[str, Any] = {}

    if page.page_header:
        ph = page.page_header
        result["page_header"] = {
            "type": ph.type.name,
            "recno": ph.recno,
            "write_gen": ph.write_gen,
            "mem_size": ph.mem_size,
            "entries": ph.entries,
            "flags": str(ph.flags),
            "version": ph.version,
        }

    if page.block_header:
        bh = page.block_header
        if isinstance(bh, btree.BlockDisaggHeader):
            result["block_header"] = {
                "type": "disagg",
                "magic": hex(bh.magic),
                "version": bh.version,
                "compatible_version": bh.compatible_version,
                "header_size": bh.header_size,
                "checksum": bh.checksum,
                "previous_checksum": bh.previous_checksum,
                "flags": str(bh.flags),
            }
        else:
            result["block_header"] = {
                "type": "standard",
                "disk_size": bh.disk_size,
                "checksum": bh.checksum,
                "flags": str(bh.flags),
            }

    if page.cells is not None:
        result["cells"] = [_cell_to_dict(cell) for cell in page.cells]

    if page.extents is not None:
        result["extents"] = [
            {"offset": ext.offset, "size": ext.size}
            for ext in page.extents
        ]

    if page.pagestats:
        ps = page.pagestats
        result["stats"] = {
            "num_keys": ps.num_keys,
            "keys_size": ps.keys_sz,
            "num_timestamps": ps.num_ts,
            "timestamps_size": ps.ts_sz,
            "num_transactions": ps.num_txn,
            "transactions_size": ps.txn_sz,
        }

    return result


def _cell_to_dict(cell: btree.Cell) -> dict[str, Any]:
    """Convert a Cell to a JSON-serializable dictionary."""
    d: dict[str, Any] = {
        "descriptor": hex(cell.descriptor),
        "type": cell.cell_type.name if cell.cell_type else _short_type_name(cell),
        "size": len(cell.data),
        "data_hex": cell.data.hex(),
    }

    # Try UTF-8 decode
    try:
        d["data_utf8"] = cell.data.decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        d["data_utf8"] = None

    if cell.extra_descriptor:
        d["extra_descriptor"] = hex(cell.extra_descriptor)

    if cell.prefix is not None:
        d["prefix_len"] = cell.prefix

    if cell.run_length is not None:
        d["run_length"] = cell.run_length

    if cell.delta_flag is not None:
        d["delta_flag"] = cell.delta_flag

    # Timestamps
    ts: dict[str, Any] = {}
    if cell.start_ts is not None:
        ts["start_ts"] = cell.start_ts
    if cell.start_txn is not None:
        ts["start_txn"] = cell.start_txn
    if cell.durable_start_ts is not None:
        ts["durable_start_ts"] = cell.durable_start_ts
    if cell.stop_ts is not None:
        ts["stop_ts"] = cell.stop_ts
    if cell.stop_txn is not None:
        ts["stop_txn"] = cell.stop_txn
    if cell.durable_stop_ts is not None:
        ts["durable_stop_ts"] = cell.durable_stop_ts
    if ts:
        d["timestamps"] = ts

    # Flags
    flags = []
    if cell.is_key:
        flags.append("key")
    if cell.is_value:
        flags.append("value")
    if cell.is_address:
        flags.append("address")
    if cell.is_overflow:
        flags.append("overflow")
    if cell.is_short:
        flags.append("short")
    if flags:
        d["flags"] = flags

    return d


def _short_type_name(cell: btree.Cell) -> str:
    """Derive type name for short cells that don't have a CellType."""
    if cell.is_key and cell.is_short:
        return "WT_CELL_KEY_SHORT" if cell.prefix is None else "WT_CELL_KEY_SHORT_PFX"
    if cell.is_value and cell.is_short:
        return "WT_CELL_VALUE_SHORT"
    return "UNKNOWN"


class JsonFormatter:
    """Write pages as a JSON array to a stream."""

    def __init__(self, stream: TextIO, *, indent: int = 2):
        self._stream = stream
        self._indent = indent
        self._pages: list[dict] = []

    def write_page(self, page: btree.WTPage, page_number: int = 0, file_offset: int = 0):
        d = page_to_dict(page)
        d["page_number"] = page_number
        d["file_offset"] = file_offset
        self._pages.append(d)

    def finish(self):
        json.dump(self._pages, self._stream, indent=self._indent)
        self._stream.write("\n")


class JsonlFormatter:
    """Write pages as JSONL (one JSON object per line) to a stream."""

    def __init__(self, stream: TextIO):
        self._stream = stream

    def write_page(self, page: btree.WTPage, page_number: int = 0, file_offset: int = 0):
        d = page_to_dict(page)
        d["page_number"] = page_number
        d["file_offset"] = file_offset
        self._stream.write(json.dumps(d))
        self._stream.write("\n")

    def finish(self):
        pass
