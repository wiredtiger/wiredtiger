"""
WiredTiger page and file decoder.

The top-level package re-exports core symbols for convenience.
Importing this module requires zero external dependencies.
"""

from wt_decode.core.btree import WTPage, Cell, DisaggAddr
from wt_decode.core.binary import BinaryFile
from wt_decode.core.options import DecodeOptions
from wt_decode.core.file_decoder import wtdecode_file_object
from wt_decode.core.btree import (
    PageType, PageFlags, PageHeader,
    BlockHeader, BlockDisaggHeader,
    CellType, ExtentItem,
)

__all__ = [
    "WTPage", "Cell", "DisaggAddr", "BinaryFile", "DecodeOptions",
    "wtdecode_file_object",
    "PageType", "PageFlags", "PageHeader",
    "BlockHeader", "BlockDisaggHeader",
    "CellType", "ExtentItem",
]
