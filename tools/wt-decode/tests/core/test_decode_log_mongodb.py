#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

import contextlib
import io
import logging
import os
import unittest

from wt_decode.core.options import DecodeOptions
from wt_decode.core.log_parser import process_logs


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "fixtures", "binary_files")


class TestDecodeMongoDBLog(unittest.TestCase):
    """Unit tests for decoding hex dumps from MongoDB logs."""

    def setUp(self):
        self.binary_files_dir = FIXTURES_DIR

    def run_decode(self, filename):
        log_path = os.path.join(self.binary_files_dir, filename)
        self.assertTrue(os.path.exists(log_path), f"Missing log file at {log_path}")

        buffer = io.StringIO()
        with self.assertLogs("wt_decode.core.log_parser", level=logging.INFO) as logs:
            with contextlib.redirect_stdout(buffer):
                with open(log_path, 'r') as f:
                    process_logs(f, DecodeOptions(dumpin=True))
        return buffer.getvalue(), "\n".join(logs.output)

    def test_decode_log_mongodb_valid(self):
        self.skipTest("FIXME-WT-16726 Test for valid MongoDB log")

    def test_decode_log_mongodb_multi_chunk_valid(self):
        self.skipTest("FIXME-WT-16726 Test for valid multi-chunk MongoDB log")

    def test_decode_log_mongodb_incomplete_chunks(self):
        self.skipTest("FIXME-WT-16726 Test for incomplete chunks in MongoDB log")

    def test_decode_log_mongodb_no_checksum_mismatch(self):
        self.skipTest("FIXME-WT-16726 Test for no checksum mismatch in MongoDB log")

    def test_decode_log_mongodb_non_hex_characters(self):
        _, logged_output = self.run_decode("mongodb_non_hex.log")

        self.assertIn("Hex dump is corrupt", logged_output)
        self.assertIn("Non-hex characters found", logged_output)
        self.assertIn("No valid byte dump found in MongoDB log", logged_output)

    def test_decode_log_mongodb_odd_length_hex(self):
        _, logged_output = self.run_decode("mongodb_odd_length.log")

        self.assertIn("Hex dump is corrupt", logged_output)
        self.assertIn("Hex data chunk length is not even", logged_output)
        self.assertIn("No valid byte dump found in MongoDB log", logged_output)

    def test_decode_log_mongodb_block_size_mismatch(self):
        _, logged_output = self.run_decode("mongodb_size_mismatch.log")

        self.assertIn("Hex dump is corrupt", logged_output)
        self.assertIn("Block size mismatch", logged_output)
        self.assertIn("No valid byte dump found in MongoDB log", logged_output)

if __name__ == "__main__":
    unittest.main()
