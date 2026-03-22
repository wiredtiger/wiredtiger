import argparse
import io
import sys
import logging
from typing import Any

from wt_decode.core import binary, btree
from wt_decode.output.text import print_page as _print_page

logger = logging.getLogger(__name__)

def make_decode_opts(
    verbose: bool = True,
    bson: bool = False,
    disagg: bool = True,
    debug: bool = False,
    is_delta: bool = False,
) -> argparse.Namespace:
    """Build a minimal argparse.Namespace matching what WTPage.parse and print_page expect.

    Args:
        is_delta: When True, signals that the bytes being decoded are a disaggregated
                  storage delta page (WT_BLOCK_DISAGG_MAGIC_DELTA) rather than a full
                  base image.  Pass is_delta=True when decoding individual delta pages
                  so that call-sites can distinguish the two decoding contexts.
    """
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
        is_delta=is_delta,
    )

def decode_page_bytes(page_bytes: bytes, opts: argparse.Namespace) -> btree.WTPage:
    """Decode raw (decrypted) page bytes into a WTPage."""
    b = binary.BinaryFile(io.BytesIO(page_bytes))
    page = btree.WTPage.parse(b, len(page_bytes),
                              disagg=getattr(opts, 'disagg', True),
                              skip_data=getattr(opts, 'skip_data', False),
                              cont=getattr(opts, 'cont', False))
    return page

def get_page_type_name(page: btree.WTPage) -> str:
    """Return a human-readable page type string."""
    if page.page_header is None:
        return "UNKNOWN"
    return page.page_header.type.name

def extract_children(page: btree.WTPage) -> list[dict[str, Any]]:
    """Extract child page references from an internal page.

    Internal pages (WT_PAGE_ROW_INT) contain address cells with DisaggAddr
    cookies that describe child pages.
    """
    children: list[dict[str, Any]] = []
    if page.page_header is None:
        return children
    if page.page_header.type != btree.PageType.WT_PAGE_ROW_INT:
        return children
    if page.cells is None:
        return children

    for cell in page.cells:
        if cell.is_address and cell.data:
            try:
                addr = btree.DisaggAddr.parse(cell.data)
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

def capture_page_text(page: btree.WTPage, opts: argparse.Namespace) -> str:
    """Capture the text output of print_page() by redirecting stdout."""
    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    try:
        _print_page(page, split=getattr(opts, 'split', False),
                    decode_as_bson=getattr(opts, 'bson', False),
                    disagg=getattr(opts, 'disagg', True))
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()
