#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

import io
import json
import os
import unittest

from wt_decode.core import btree, binary
from wt_decode.core.log_parser import encode_bytes
from wt_decode.output.json import page_to_dict, JsonFormatter, JsonlFormatter


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "fixtures", "binary_files")


class TestJsonOutput(unittest.TestCase):
    """Tests for JSON/JSONL output formatting."""

    def _load_page(self):
        file_path = os.path.join(FIXTURES_DIR, "WiredTiger01.txt")
        with open(file_path, "r", encoding="utf-8") as f:
            page_bytes = encode_bytes(f)
        return btree.WTPage.parse(binary.BinaryFile(io.BytesIO(page_bytes)), len(page_bytes))

    def test_page_to_dict_has_expected_keys(self):
        page = self._load_page()
        d = page_to_dict(page)

        self.assertIn("page_header", d)
        self.assertIn("block_header", d)
        self.assertIn("cells", d)
        self.assertIn("stats", d)

    def test_page_header_fields(self):
        page = self._load_page()
        d = page_to_dict(page)
        ph = d["page_header"]

        self.assertEqual(ph["type"], "WT_PAGE_ROW_LEAF")
        self.assertEqual(ph["entries"], 16)
        self.assertEqual(ph["write_gen"], 11)

    def test_cells_count(self):
        page = self._load_page()
        d = page_to_dict(page)

        self.assertEqual(len(d["cells"]), 16)

    def test_cell_has_data_hex(self):
        page = self._load_page()
        d = page_to_dict(page)

        for cell in d["cells"]:
            self.assertIn("data_hex", cell)

    def test_json_formatter_output(self):
        page = self._load_page()
        stream = io.StringIO()
        fmt = JsonFormatter(stream)
        fmt.write_page(page, page_number=0, file_offset=512)
        fmt.finish()

        output = stream.getvalue()
        parsed = json.loads(output)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["page_number"], 0)
        self.assertEqual(parsed[0]["file_offset"], 512)

    def test_jsonl_formatter_output(self):
        page = self._load_page()
        stream = io.StringIO()
        fmt = JsonlFormatter(stream)
        fmt.write_page(page, page_number=0)
        fmt.write_page(page, page_number=1)
        fmt.finish()

        lines = stream.getvalue().strip().split('\n')
        self.assertEqual(len(lines), 2)
        for i, line in enumerate(lines):
            parsed = json.loads(line)
            self.assertEqual(parsed["page_number"], i)

    def test_roundtrip_serializable(self):
        """Ensure page_to_dict output is fully JSON-serializable."""
        page = self._load_page()
        d = page_to_dict(page)
        # Should not raise
        json.dumps(d)


if __name__ == "__main__":
    unittest.main()
