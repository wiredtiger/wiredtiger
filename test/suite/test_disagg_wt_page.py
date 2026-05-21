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
from helper_disagg import DisaggConfigMixin

# test_disagg_wt_page.py
#    Drive the `wt page` command end-to-end through a disagg cell built
#    with the in-tree palite page-log extension.
class test_disagg_wt_page(wttest.WiredTigerTestCase, DisaggConfigMixin):
    uri_base = "wt_page_test"
    uri = "layered:" + uri_base
    stable_uri = "file:" + uri_base + ".wt_stable"
    nrows = 1000

    conn_config = 'disaggregated=(role="leader")'

    def conn_extensions(self, extlist):
        extlist.skip_if_missing = True
        DisaggConfigMixin.conn_extensions(self, extlist)

    def _wt_bin(self):
        build = self.buildDirectory()
        libs_wt = os.path.join(build, '.libs', 'wt')
        if os.path.isfile(libs_wt):
            return libs_wt
        return os.path.join(build, 'wt')

    def _palite_ext(self):
        build = self.buildDirectory()
        matches = glob.glob(
            os.path.join(build, 'ext', 'page_log', 'palite', '**', 'libwiredtiger_palite.so'),
            recursive=True)
        if matches:
            return matches[0]
        self.fail("Could not locate libwiredtiger_palite.so in build directory")

    def _wt_page_extra_config(self):
        ext = self._palite_ext()
        return f'extensions=["{ext}"],disaggregated=(page_log=palite,role="follower")'

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _root_page_id(self):
        """Decode root page_id from the disagg checkpoint cookie in metadata.

        Cookie wire format (see block_disagg_addr.c __wti_block_disagg_addr_pack):
          prefix : 4b-packed (version, version_min) pair, currently 0x00 (one byte)
          varint : page_id           <-- target
          varint : flags
          ...    : lsn, base_lsn_delta, size, checksum

        WT unsigned varint (intpack_inline.h):
          0x80-0xBF  1 byte:  value = byte & 0x3F          (0-63)
          0xC0-0xDF  2 bytes: value = ((b & 0x1F)<<8|b2)+64 (64-8255)
        """
        meta_c = self.session.open_cursor('metadata:')
        meta_c.set_key(self.stable_uri)
        self.assertEqual(meta_c.search(), 0, f"URI not found in metadata: {self.stable_uri}")
        val = meta_c.get_value()
        meta_c.close()

        m = re.search(r'\bcheckpoint=\([^)]*addr="([0-9a-f]+)"', val)
        if m is None:
            m = re.search(r'\baddr="([0-9a-f]+)"', val)
        self.assertIsNotNone(m, f"No addr= found in metadata for {self.stable_uri}: {val}")
        data = bytes.fromhex(m.group(1))

        # Skip the 4b-packed version prefix (two nibbles, low first). Each nibble's
        # high bit is the continuation flag; iterate until we've consumed two values.
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

        b = data[byte_pos]
        if 0x80 <= b <= 0xBF:
            return b & 0x3F
        if 0xC0 <= b <= 0xDF:
            return (((b & 0x1F) << 8) | data[byte_pos + 1]) + 64
        self.fail(f"Unexpected varint lead byte 0x{b:02x} at offset {byte_pos}")

    def _run_wt_page(self, *args):
        self.close_conn()
        try:
            return subprocess.run(
                [self._wt_bin(), "-h", self.home, "-C", self._wt_page_extra_config(),
                 "page", *args],
                capture_output=True, text=True, check=False)
        finally:
            self.reopen_conn()

    def test_page_subcommand_help(self):
        """`wt page -?` is plumbed in."""
        self.close_conn()
        try:
            out = subprocess.run(
                [self._wt_bin(), '-h', self.home, 'page', '-?'],
                capture_output=True, text=True, check=False)
            self.assertEqual(out.returncode, 0, msg=out.stderr)
            self.assertIn('page -p page_id', out.stderr)
        finally:
            self.reopen_conn()

    def test_happy_path_root_page(self):
        self._populate()
        page_id = self._root_page_id()
        out = self._run_wt_page("-p", str(page_id), self.stable_uri)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertRegex(out.stdout, re.compile(r'^chain: page_id=\d+ ', re.MULTILINE))
        self.assertRegex(out.stdout, re.compile(r'^- row-store ', re.MULTILINE))

    def test_unknown_page_id(self):
        self._populate()
        out = self._run_wt_page("-p", "99999999", self.stable_uri)
        self.assertNotEqual(out.returncode, 0)

    def test_missing_required_p(self):
        self._populate()
        out = self._run_wt_page(self.stable_uri)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("page", out.stderr.lower())

    def test_lsn_ahead_of_frontier(self):
        self._populate()
        page_id = self._root_page_id()
        far_future_lsn = 10 ** 12
        out = self._run_wt_page("-p", str(page_id), "-l", str(far_future_lsn), self.stable_uri)
        self.assertNotIn("PANIC", out.stderr)

if __name__ == '__main__':
    wttest.run()
