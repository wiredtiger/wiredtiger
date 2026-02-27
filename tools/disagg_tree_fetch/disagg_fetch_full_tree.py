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

"""Fetch, decrypt, decode, and traverse a disaggregated page tree.

Given a root page tuple (log_id, table_id, page_id, lsn), this script:
1. Fetches each page via the PageService gRPC API (native Python grpcio).
2. Decrypts each page via the pagedecryptor command-line tool.
3. Decodes each page using the py_common library (WTPage.parse) directly.
4. Extracts child page tuples from decoded internal pages.
5. Repeats until the full reachable tree is fetched.

Artifacts are written to an output directory:
- pages/page_<page_id>_lsn_<lsn>.json
- decrypted/decrypted_<log>_<table>_<page>_<lsn>[_delta_N|_fullimage].bin
- decoded/decoded_page_<page_id>_lsn_<lsn>.txt
- manifest.json (summary of all visited pages)

Prerequisites:
- Generate proto stubs: ./generate_proto.sh  (or run it once manually)
- pagedecryptor binary on PATH or specified via --decryptor-path
- A valid encryption key file (--key-file)
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import logging
import subprocess
import sys
import tempfile
from collections import deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Ensure py_common and the generated gRPC stubs are importable.
# ---------------------------------------------------------------------------
_TOOLS_DIR = Path(__file__).resolve().parent.parent
_SELF_DIR = Path(__file__).resolve().parent

sys.path.insert(0, str(_TOOLS_DIR))
sys.path.insert(0, str(_SELF_DIR))

import grpc
from pageservice.v1 import page_service_pb2, page_service_pb2_grpc

from py_common import binary_data, btree_format
from py_common.printer import Printer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PageTuple:
    """Identifies a single page version in the page service."""
    page_id: int
    lsn: int


@dataclass
class DecodedPageInfo:
    """Summary information collected for each visited page."""
    page_id: int
    lsn: int
    page_type: str = "UNKNOWN"
    write_gen: Optional[int] = None
    decrypted_size: int = 0
    num_children: int = 0
    children: list[dict[str, Any]] = field(default_factory=list)
    # File paths of saved artifacts.
    page_json_path: str = ""
    decrypted_path: str = ""
    decoded_path: str = ""
    has_deltas: bool = False
    num_deltas: int = 0


# ---------------------------------------------------------------------------
# gRPC page fetching
# ---------------------------------------------------------------------------

def create_page_service_stub(page_server: str) -> page_service_pb2_grpc.PageServiceStub:
    """Create a gRPC stub for the PageService."""
    channel = grpc.insecure_channel(page_server)
    return page_service_pb2_grpc.PageServiceStub(channel)


def fetch_page(
    stub: page_service_pb2_grpc.PageServiceStub,
    log_id: int,
    table_id: int,
    page_id: int,
    lsn: int,
) -> page_service_pb2.GetPageAtLSNResponse:
    """Fetch a page via the unary GetPageAtLSN RPC."""
    request = page_service_pb2.GetPageAtLSNRequest(
        log_id=log_id,
        table_id=table_id,
        page_id=page_id,
        lsn=lsn,
    )
    return stub.GetPageAtLSN(request)


def page_to_json_dict(page: page_service_pb2.Page) -> dict[str, Any]:
    """Convert a protobuf Page message to a JSON-serializable dict for artifact storage."""
    return {
        "log_id": page.log_id,
        "table_id": page.table_id,
        "page_id": page.page_id,
        "lsn": page.lsn,
        "contents_b64": base64.b64encode(page.contents).decode("ascii"),
        "contents_size": len(page.contents),
        "deltas_b64": [base64.b64encode(d).decode("ascii") for d in page.deltas],
        "delta_sizes": [len(d) for d in page.deltas],
        "base_lsn": page.base_lsn,
        "lsns": list(page.lsns),
        "backlinks": list(page.backlinks),
        "full_image_lsn": page.full_image_lsn,
        "full_image_backlink_lsn": page.full_image_backlink_lsn,
    }

# ---------------------------------------------------------------------------
# Decryption (subprocess — pagedecryptor CLI)
# ---------------------------------------------------------------------------

def decrypt_bytes(
    decryptor_path: str,
    key_file: str,
    encrypted_bytes: bytes,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    backlink_lsn: Optional[int] = None,
    base_lsn: Optional[int] = None,
    is_delta: bool = False,
    output_path: Optional[Path] = None,
    debug: bool = False,
) -> bytes:
    """Decrypt page bytes using the pagedecryptor CLI tool.

    The tool expects a file containing a base64-encoded byte string as input and
    writes the decrypted binary to the output path.  If *output_path* is given
    the decrypted file is persisted there; otherwise a temporary file is used.
    """
    b64_data = base64.b64encode(encrypted_bytes).decode("ascii")

    # If a persistent output path is requested, write there; otherwise use a
    # temporary file pair.
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".in", delete=True) as tmp_in:
            tmp_in.write(b64_data)
            tmp_in.flush()
            _run_decryptor(
                decryptor_path, key_file, tmp_in.name, str(output_path),
                lsn, table_id, page_id,
                backlink_lsn=backlink_lsn, base_lsn=base_lsn,
                is_delta=is_delta, debug=debug,
            )
        return output_path.read_bytes()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".in", delete=True) as tmp_in, \
         tempfile.NamedTemporaryFile(mode="rb", suffix=".out", delete=True) as tmp_out:
        tmp_in.write(b64_data)
        tmp_in.flush()
        _run_decryptor(
            decryptor_path, key_file, tmp_in.name, tmp_out.name,
            lsn, table_id, page_id,
            backlink_lsn=backlink_lsn, base_lsn=base_lsn,
            is_delta=is_delta, debug=debug,
        )
        return tmp_out.read()


def _run_decryptor(
    decryptor_path: str,
    key_file: str,
    input_path: str,
    output_path: str,
    lsn: int,
    table_id: int,
    page_id: int,
    *,
    backlink_lsn: Optional[int] = None,
    base_lsn: Optional[int] = None,
    is_delta: bool = False,
    debug: bool = False,
) -> None:
    cmd = [
        decryptor_path,
        "--inputPath", input_path,
        "--outputPath", output_path,
        "--keyFile", key_file,
        "--lsn", str(lsn),
        "--tableId", str(table_id),
        "--pageId", str(page_id),
    ]
    if backlink_lsn is not None:
        cmd.extend(["--backlinkLsn", str(backlink_lsn)])
    if base_lsn is not None:
        cmd.extend(["--baseLsn", str(base_lsn)])
    if is_delta:
        cmd.append("--isDelta")

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    if debug:
        if result.stdout:
            logger.debug("pagedecryptor stdout: %s", result.stdout)
        if result.stderr:
            logger.debug("pagedecryptor stderr: %s", result.stderr)


# ---------------------------------------------------------------------------
# Page decoding (using py_common library directly)
# ---------------------------------------------------------------------------

def make_decode_opts(
    verbose: bool = True,
    bson: bool = False,
    disagg: bool = True,
    debug: bool = False,
) -> argparse.Namespace:
    """Build a minimal argparse.Namespace matching what WTPage.parse and print_page expect."""
    return argparse.Namespace(
        verbose=verbose,
        split=False,
        ext=False,
        fragment=True,
        disagg=disagg,
        bson=bson,
        debug=debug,
        cont=False,
        output=None,
        skip_data=False,
        offset=0,
        pages=0,
    )


def decode_page_bytes(page_bytes: bytes, opts: argparse.Namespace) -> btree_format.WTPage:
    """Decode raw (decrypted) page bytes into a WTPage."""
    b = binary_data.BinaryFile(io.BytesIO(page_bytes))
    page = btree_format.WTPage()
    page = page.parse(b, len(page_bytes), opts)
    return page


def get_page_type_name(page: btree_format.WTPage) -> str:
    """Return a human-readable page type string."""
    if page.page_header is None:
        return "UNKNOWN"
    return page.page_header.type.name


def extract_children(page: btree_format.WTPage) -> list[dict[str, Any]]:
    """Extract child page references from an internal page.

    Internal pages (WT_PAGE_ROW_INT) contain address cells with DisaggAddr
    cookies that describe child pages.
    """
    children: list[dict[str, Any]] = []
    if page.page_header is None:
        return children
    if page.page_header.type != btree_format.PageType.WT_PAGE_ROW_INT:
        return children
    if page.cells is None:
        return children

    for cell in page.cells:
        if cell.is_address and cell.data:
            try:
                addr = btree_format.DisaggAddr.parse(cell.data)
                children.append({
                    "page_id": addr.page_id,
                    "flags": int(addr.flags),
                    "lsn": addr.lsn,
                    "base_lsn": addr.base_lsn,
                    "size": addr.size,
                    "checksum": addr.checksum,
                })
            except Exception as exc:
                logger.warning("Failed to parse DisaggAddr from cell data: %s", exc)
    return children


def capture_page_text(page: btree_format.WTPage, opts: argparse.Namespace) -> str:
    """Capture the text output of page.print_page() by redirecting stdout."""
    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        page.print_page(opts)
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tree traversal
# ---------------------------------------------------------------------------

def traverse_tree(args: argparse.Namespace) -> int:
    """Main traversal loop: fetch → decrypt → decode → find children → repeat."""
    decode_opts = make_decode_opts(
        verbose=args.verbose,
        bson=args.bson,
        disagg=True,
        debug=args.debug,
    )

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(f"/tmp/disagg_tree_{args.table_id}_{args.root_page_id}_{args.root_lsn}")
    )
    pages_dir = output_dir / "pages"
    decrypted_dir = output_dir / "decrypted"
    decoded_dir = output_dir / "decoded"
    for d in (pages_dir, decrypted_dir, decoded_dir):
        d.mkdir(parents=True, exist_ok=True)

    stub = create_page_service_stub(args.page_server)
    root = PageTuple(page_id=args.root_page_id, lsn=args.root_lsn)
    queue: deque[PageTuple] = deque([root])
    visited: set[PageTuple] = set()
    manifest_pages: list[dict[str, Any]] = []

    print(
        f"Starting tree fetch from root page_id={root.page_id}, lsn={root.lsn}, "
        f"table_id={args.table_id}, log_id={args.log_id}"
    )
    print(f"Artifacts will be written to: {output_dir}")

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        if args.max_pages and len(visited) >= args.max_pages:
            print(f"Reached --max-pages={args.max_pages}, stopping early.")
            break

        print(f"\nFetching page_id={current.page_id} lsn={current.lsn} ...")
        visited.add(current)

        # ------------------------------------------------------------------
        # 1. Fetch via gRPC
        # ------------------------------------------------------------------
        try:
            response = fetch_page(stub, args.log_id, args.table_id, current.page_id, current.lsn)
        except grpc.RpcError as exc:
            logger.error("gRPC error fetching page_id=%d lsn=%d: %s", current.page_id, current.lsn, exc)
            continue

        page_proto = response.page

        # Save the raw page JSON for debugging.
        page_json_dict = page_to_json_dict(page_proto)
        page_json_path = pages_dir / f"page_{current.page_id}_lsn_{current.lsn}.json"
        page_json_path.write_text(json.dumps(page_json_dict, indent=2))

        deltas = list(page_proto.deltas)
        decoded_text_parts: list[str] = []

        # ------------------------------------------------------------------
        # 2. Decrypt & 3. Decode
        # ------------------------------------------------------------------
        if deltas:
            # --- Full image ---
            full_image_lsn = page_proto.full_image_lsn
            full_image_backlink_lsn = page_proto.full_image_backlink_lsn

            full_output = (
                decrypted_dir
                / f"decrypted_{args.log_id}_{args.table_id}_{current.page_id}_{current.lsn}_fullimage.bin"
            )
            full_bytes = decrypt_bytes(
                args.decryptor_path, args.key_file,
                page_proto.contents,
                full_image_lsn, args.table_id, current.page_id,
                backlink_lsn=full_image_backlink_lsn,
                output_path=full_output,
                debug=args.debug,
            )
            full_page = decode_page_bytes(full_bytes, decode_opts)
            decoded_text_parts.append(
                f"=== full_image lsn={full_image_lsn} ===\n"
                + capture_page_text(full_page, decode_opts)
            )

            # --- Deltas ---
            lsns = list(page_proto.lsns)
            backlinks = list(page_proto.backlinks)
            base_lsn = page_proto.base_lsn

            for idx, delta_bytes_enc in enumerate(deltas):
                delta_lsn = lsns[idx] if idx < len(lsns) else 0
                delta_backlink = backlinks[idx] if idx < len(backlinks) else 0

                delta_output = (
                    decrypted_dir
                    / f"decrypted_{args.log_id}_{args.table_id}_{current.page_id}_{current.lsn}_delta_{idx}.bin"
                )
                delta_bytes = decrypt_bytes(
                    args.decryptor_path, args.key_file,
                    delta_bytes_enc,
                    delta_lsn, args.table_id, current.page_id,
                    backlink_lsn=delta_backlink,
                    base_lsn=base_lsn,
                    is_delta=True,
                    output_path=delta_output,
                    debug=args.debug,
                )
                delta_page = decode_page_bytes(delta_bytes, decode_opts)
                decoded_text_parts.append(
                    f"=== delta_{idx} lsn={delta_lsn} backlink={delta_backlink} ===\n"
                    + capture_page_text(delta_page, decode_opts)
                )

            # Use the full image for child extraction.
            decoded_page = full_page
            decrypted_path = full_output
            decoded_text = "\n\n".join(decoded_text_parts)
        else:
            # No deltas — just the base page.
            decrypted_path = (
                decrypted_dir
                / f"decrypted_{args.log_id}_{args.table_id}_{current.page_id}_{current.lsn}.bin"
            )
            page_bytes = decrypt_bytes(
                args.decryptor_path, args.key_file,
                page_proto.contents,
                current.lsn, args.table_id, current.page_id,
                output_path=decrypted_path,
                debug=args.debug,
            )
            decoded_page = decode_page_bytes(page_bytes, decode_opts)
            decoded_text = capture_page_text(decoded_page, decode_opts)

        # ------------------------------------------------------------------
        # 4. Save decoded output
        # ------------------------------------------------------------------
        decoded_path = decoded_dir / f"decoded_page_{current.page_id}_lsn_{current.lsn}.txt"
        decoded_path.write_text(decoded_text)

        page_type = get_page_type_name(decoded_page)
        write_gen = (
            decoded_page.page_header.write_gen
            if decoded_page.page_header is not None
            else None
        )

        # ------------------------------------------------------------------
        # 5. Extract children & enqueue
        # ------------------------------------------------------------------
        children = extract_children(decoded_page)
        for child in children:
            child_tuple = PageTuple(page_id=child["page_id"], lsn=child["lsn"])
            if child_tuple not in visited:
                queue.append(child_tuple)

        info = DecodedPageInfo(
            page_id=current.page_id,
            lsn=current.lsn,
            page_type=page_type,
            write_gen=write_gen,
            decrypted_size=decrypted_path.stat().st_size,
            num_children=len(children),
            children=children,
            page_json_path=str(page_json_path),
            decrypted_path=str(decrypted_path),
            decoded_path=str(decoded_path),
            has_deltas=bool(deltas),
            num_deltas=len(deltas),
        )
        manifest_pages.append(asdict(info))

        print(
            f"  -> type={page_type}, write_gen={write_gen}, "
            f"decrypted_size={info.decrypted_size}, "
            f"children={len(children)}, deltas={len(deltas)}"
        )

    # ------------------------------------------------------------------
    # Write manifest
    # ------------------------------------------------------------------
    manifest = {
        "root": {
            "log_id": args.log_id,
            "table_id": args.table_id,
            "page_id": args.root_page_id,
            "lsn": args.root_lsn,
        },
        "pages_visited": len(manifest_pages),
        "output_dir": str(output_dir),
        "pages": manifest_pages,
    }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"\nDone. Visited {len(manifest_pages)} pages.")
    print(f"Manifest: {manifest_path}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch/decrypt/decode a full disaggregated page tree from a root page.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Required identifiers.
    parser.add_argument("--log-id", type=int, required=True, help="SLS log ID (shard)")
    parser.add_argument("--table-id", type=int, required=True, help="WiredTiger table ID")
    parser.add_argument("--root-page-id", type=int, required=True, help="Root page ID to start traversal")
    parser.add_argument("--root-lsn", type=int, required=True, help="LSN of the root page")

    # Connection / tool paths.
    parser.add_argument("--page-server", default="172.17.0.1:20044",
                        help="Address of the PageService gRPC server (default: %(default)s)")
    parser.add_argument(
        "--decryptor-path", default="pagedecryptor",
        help="Path to the pagedecryptor binary (default: %(default)s)",
    )
    parser.add_argument("--key-file", default="/data/db/job0/mongorunner/decrypt_key",
                        help="Path to the encryption key file (default: %(default)s)")

    # Decoding options.
    parser.add_argument("--verbose", action="store_true", default=True,
                        help="Print cell data, not just headers (default: True)")
    parser.add_argument("--no-verbose", action="store_false", dest="verbose",
                        help="Print only page/block headers")
    parser.add_argument("--bson", action="store_true", default=False,
                        help="Decode cell values as BSON")

    # Output & limits.
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (default: /tmp/disagg_tree_<table>_<rootpage>_<rootlsn>)")
    parser.add_argument("--max-pages", type=int, default=0,
                        help="Safety limit on pages to visit. 0 = no limit (default: 0)")

    # Debug.
    parser.add_argument("--debug", action="store_true", default=False,
                        help="Enable debug logging")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO", help="Logging level (default: %(default)s)")

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    level = "DEBUG" if args.debug else args.log_level
    logging.basicConfig(level=level, format="[%(levelname)s] %(name)s: %(message)s")

    return traverse_tree(args)


if __name__ == "__main__":
    raise SystemExit(main())
