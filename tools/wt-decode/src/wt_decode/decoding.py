import argparse
import io
import sys
import logging
from typing import Any

from py_common import binary_data, btree_format

logger = logging.getLogger(__name__)

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
