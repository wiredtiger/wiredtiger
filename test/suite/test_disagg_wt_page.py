#!/usr/bin/env python
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

import glob, os, re, subprocess
import wttest

# test_disagg_wt_page.py
#    Drive the `wt page` command end-to-end through a disagg cell.
class test_disagg_wt_page(wttest.WiredTigerTestCase):
    uri = "file:wt_page_test.wt_stable"
    nrows = 10_000

    conn_config = 'disaggregated=(page_log=palite),disaggregated=(role="leader")'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignoreStdoutPattern('WT_VERB_RTS')

    def conn_extensions(self, extlist):
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', 'palite')

    def early_setup(self):
        os.mkdir('kv_home')

    def _wt_bin(self):
        build = self.buildDirectory()
        libs_wt = os.path.join(build, '.libs', 'wt')
        if os.path.isfile(libs_wt):
            return libs_wt
        return os.path.join(build, 'wt')

    def _palite_ext(self):
        build = self.buildDirectory()
        # CMake layout: ext/page_log/palite/libwiredtiger_palite.so
        # Libtool layout: ext/page_log/palite/.libs/libwiredtiger_palite.so
        for pat in [
            os.path.join(build, 'ext', 'page_log', 'palite', 'libwiredtiger_palite.so'),
            os.path.join(build, 'ext', 'page_log', 'palite', '.libs', 'libwiredtiger_palite.so'),
        ]:
            if os.path.exists(pat):
                return pat
        matches = glob.glob(
            os.path.join(build, 'ext', 'page_log', 'palite', '**', 'libwiredtiger_palite.so'),
            recursive=True)
        if matches:
            return matches[0]
        self.fail("Could not locate libwiredtiger_palite.so in build directory")

    def _wt_page_extra_config(self):
        ext = self._palite_ext()
        return f'extensions=["{ext}"],disaggregated=(page_log=palite)'

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _root_page_id(self):
        """
        Decode the root page_id from the disagg checkpoint cookie stored in the
        metadata for self.uri.

        Cookie wire format (see block_disagg_addr.c __wt_block_disagg_addr_pack):
          - 4b-packed pair (version, version_min): two nibbles, currently both 0  byte 0x00
          - WT unsigned varint: page_id
          - WT unsigned varint: flags
          - ... (lsn, base_lsn_delta, size, checksum)

        4b nibble encoding (int4bitpack_inline.h): bits[2:0] = value chunk, bit[3] = continuation
        (two nibbles per byte, low nibble first).

        WT unsigned varint (intpack_inline.h):
          0x80-0xBF   1 byte, value = byte & 0x3F            (0-63)
          0xC0-0xDF   2 bytes, value = ((b & 0x1F)<<8|b2)+64 (64-8255)
          0xE0+       larger values (not needed here)
        """
        meta_c = self.session.open_cursor('metadata:')
        meta_c.set_key(self.uri)
        self.assertEqual(meta_c.search(), 0, "URI not found in metadata")
        val = meta_c.get_value()
        meta_c.close()

        m = re.search(r'\baddr="([0-9a-f]+)"', val)
        self.assertIsNotNone(m, f"No addr= found in metadata for {self.uri}")
        data = bytes.fromhex(m.group(1))

        # Skip the 4b-packed version prefix (two nibbles = 1 byte when both are 0).
        # Advance past both nibble-encoded ints (generic loop).
        nibble_pos = 0
        for _ in range(2):
            while True:
                byte_idx = nibble_pos // 2
                nibble_idx = nibble_pos % 2
                nibble = (data[byte_idx] >> (nibble_idx * 4)) & 0xF
                nibble_pos += 1
                if not (nibble & 0x8):
                    break
        byte_pos = (nibble_pos + 1) // 2

        # Decode the first WT unsigned varint: page_id.
        b = data[byte_pos]
        if b <= 0xBF:
            page_id = b & 0x3F
        elif b <= 0xDF:
            page_id = ((b & 0x1F) << 8) | data[byte_pos + 1]
            page_id += 64
        else:
            self.fail(f"Unexpected varint lead byte 0x{b:02x} at offset {byte_pos}")

        return page_id

    def test_happy_path_root_page(self):
        self._populate()
        page_id = self._root_page_id()
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", "-p", str(page_id), self.uri],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertIn("=== wt page:", out.stdout)
        self.assertIn("=== chain:", out.stdout)
        self.assertIn("--- result 0 (base", out.stdout)

    def test_unknown_page_id(self):
        self._populate()
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", "-p", "99999999", self.uri],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()
        self.assertNotEqual(out.returncode, 0)

    def test_missing_required_p(self):
        self._populate()
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", self.uri],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("page", out.stderr.lower())

    def test_lsn_ahead_of_frontier(self):
        self._populate()
        # The frontier sits at the most recent materialized LSN; passing a far-future LSN
        # should not crash and should still return data (palite returns the chain DESC
        # from the cap), but the BM should emit the WT_VERB_DISAGGREGATED_STORAGE warning.
        page_id = self._root_page_id()
        far_future_lsn = 10 ** 12
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), "-v", "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", "-p", str(page_id), "-l", str(far_future_lsn), self.uri],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()
        # Either ret 0 with data, or non-zero  the contract is "no panic, no crash".
        self.assertNotIn("PANIC", out.stderr)

if __name__ == '__main__':
    wttest.run()
