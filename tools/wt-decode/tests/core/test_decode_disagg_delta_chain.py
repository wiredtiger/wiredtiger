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

import contextlib
import io
import os
import unittest

from wt_decode.core.options import DecodeOptions
from wt_decode.core.log_parser import process_logs


FIXTURES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "fixtures", "binary_files")


class TestDecodeDisaggDeltaChain(unittest.TestCase):
    """Unit test for decoding the disagg delta chain log."""

    def test_decode_disagg_delta_chain_log(self):
        log_path = os.path.join(FIXTURES_DIR, "disagg_delta_chain.log")
        self.assertTrue(os.path.exists(log_path), f"Missing delta chain log at {log_path}")

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            with open(log_path, 'r') as f:
                process_logs(f, DecodeOptions(disagg=True, dumpin=True))
        output = buffer.getvalue()

        self.assertGreater(len(output), 0, "Decoder output should not be empty")

        # Validate the full-image block and the number of delta blocks decoded.
        self.assertIn("magic: 0xdb (full image)", output)
        self.assertEqual(output.count("magic: 0xdd (delta)"), 10)


if __name__ == "__main__":
    unittest.main()
