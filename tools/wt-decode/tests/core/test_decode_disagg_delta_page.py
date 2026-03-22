#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

import os
import unittest

from wt_decode.core import binary, btree
from wt_decode.output.text import print_page


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "fixtures", "binary_files")


class TestDecodeDeltaPage(unittest.TestCase):
    """Unit tests for decoding a delta page from disaggregated storage."""

    def test_decode_disagg_delta_page(self):
        """Decode delta page from MongoDB oplog."""
        page_path = os.path.join(FIXTURES_DIR, "disagg_delta_oplog.bin")
        self.assertTrue(os.path.exists(page_path), f"Page binary not found: {page_path}")

        with open(page_path, "rb") as disagg_file:
            b = binary.BinaryFile(disagg_file)
            nbytes = os.path.getsize(page_path)

            page = btree.WTPage.parse(b, nbytes, disagg=True)

            print_page(page, decode_as_bson=True, disagg=True)

            self.assertTrue(getattr(page, 'success', True), 'WTPage.parse failed')

            # Check page header fields
            page_header = page.page_header
            self.assertEqual(page_header.recno, 0)
            self.assertEqual(page_header.write_gen, 6)
            self.assertEqual(page_header.mem_size, 2268)
            self.assertEqual(page_header.entries, 10)
            self.assertEqual(page_header.type.name, 'WT_PAGE_ROW_LEAF')
            self.assertEqual(int(page_header.flags), 0)
            self.assertEqual(page_header.version, 0)

            # Check block disagg header
            block_header = page.block_header
            self.assertEqual(block_header.magic, btree.BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA)
            self.assertEqual(block_header.version, 1)
            self.assertEqual(block_header.compatible_version, 1)
            self.assertEqual(block_header.header_size, 44)
            self.assertEqual(block_header.checksum, 2836552602)
            self.assertEqual(block_header.previous_checksum, 4000115340)
            self.assertTrue(block_header.flags & btree.BlockDisaggFlags.WT_BLOCK_DISAGG_DATA_CKSUM)

            # There should be ten cells
            self.assertEqual(len(page.cells), 10)

            c0 = page.cells[0]
            self.assertTrue(c0.is_key)

            c1 = page.cells[1]
            self.assertTrue(c1.is_value)
            self.assertEqual(len(c1.data), 419)
            self.assertIsNotNone(c1.start_ts)
            self.assertEqual(c1.start_ts, 7613589195910545415)


if __name__ == "__main__":
    unittest.main()
