#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

import os, subprocess
import wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_disagg_wt_page.py
#    Drive the `wt page` subcommand end-to-end through a disagg cell.
class test_disagg_wt_page(wttest.WiredTigerTestCase, DisaggConfigMixin):
    uri = "file:wt_page_test.wt_stable"
    nrows = 10_000

    disagg_storages = gen_disagg_storages('test_disagg_wt_page', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def conn_config(self):
        return self.disagg_conn_config()

    def _wt_bin(self):
        # Test harness runs from build/test/suite/.../<test>/; the `wt` binary
        # sits at the top of the build directory.
        return os.path.join(os.environ.get("WT_BUILDDIR", "../../.."), "wt")

    def _populate(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        c = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            c[f"k{i:08}"] = f"v{i:08}"
        c.close()
        self.session.checkpoint()

    def _root_page_id(self):
        """
        Resolve the root page_id for self.uri. The disagg checkpoint cookie's
        first packed varint is the root page_id; see
        __wt_block_disagg_addr_unpack in src/block_disagg/block_disagg_addr.c.
        Easiest path: ask `wt list -v` to print it.
        """
        out = subprocess.run(
            [self._wt_bin(), "-h", self.home, "list", "-v", self.uri],
            capture_output=True, text=True, check=True)
        for line in out.stdout.splitlines():
            if "page_id=" in line:
                return int(line.split("page_id=", 1)[1].split()[0].rstrip(","))
        self.fail(f"could not resolve root page_id for {self.uri}; list -v output:\n{out.stdout}")

    def test_happy_path_root_page(self):
        self._populate()
        page_id = self._root_page_id()
        out = subprocess.run(
            [self._wt_bin(), "-h", self.home, "page", "-p", str(page_id), self.uri],
            capture_output=True, text=True, check=False)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertIn("=== wt page:", out.stdout)
        self.assertIn("=== chain:", out.stdout)
        self.assertIn("--- result 0 (base", out.stdout)

    def test_unknown_page_id(self):
        self._populate()
        out = subprocess.run(
            [self._wt_bin(), "-h", self.home, "page", "-p", "99999999", self.uri],
            capture_output=True, text=True, check=False)
        self.assertNotEqual(out.returncode, 0)

    def test_missing_required_p(self):
        self._populate()
        out = subprocess.run(
            [self._wt_bin(), "-h", self.home, "page", self.uri],
            capture_output=True, text=True, check=False)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("page", out.stderr.lower())

if __name__ == '__main__':
    wttest.run()
