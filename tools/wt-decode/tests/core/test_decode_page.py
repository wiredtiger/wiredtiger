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

import io
import os
import unittest

from wt_decode.core import btree, binary
from wt_decode.core.log_parser import encode_bytes


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "fixtures", "binary_files")


class Test(unittest.TestCase):
    """Unit tests for decoding a single WT page."""

    def load_page_bytes(self):
        """Read WiredTiger01.txt and convert its hex dump into raw bytes."""
        file_path = os.path.join(FIXTURES_DIR, "WiredTiger01.txt")
        with open(file_path, "r", encoding="utf-8") as f:
            return encode_bytes(f)

    def test_wtpage_headers_from_wiredtiger01(self):
        """Decode WiredTiger01.txt and verify page and block header fields."""
        page_bytes = self.load_page_bytes()
        self.assertGreater(len(page_bytes), 0, "Encoded page bytes should not be empty")

        b = binary.BinaryFile(io.BytesIO(page_bytes))

        page = btree.WTPage.parse(b, len(page_bytes), skip_data=True)
        self.assertTrue(page.success, "WTPage parsing failed")

        # Validate Page Header fields
        p = page.page_header
        self.assertIsNotNone(p)
        self.assertEqual(p.recno, 0)
        self.assertEqual(p.write_gen, 11)
        self.assertEqual(p.mem_size, 3702)
        self.assertEqual(p.entries, 16)
        self.assertEqual(p.type, btree.PageType.WT_PAGE_ROW_LEAF)
        self.assertEqual(p.flags, btree.PageFlags.WT_PAGE_EMPTY_V_NONE)
        self.assertEqual(p.version, 1)

        # Validate Block Header fields
        bh = page.block_header
        self.assertIsNotNone(bh)
        self.assertEqual(bh.disk_size, 4096)
        self.assertEqual(bh.checksum, 414598985)
        self.assertEqual(bh.flags, btree.BlockFlags.WT_BLOCK_DATA_CKSUM)


if __name__ == "__main__":
    unittest.main()
