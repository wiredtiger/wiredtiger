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

import os, subprocess
import wttest

# test_disagg_wt_page.py
#    Smoke-test the `wt page` CLI subcommand. Full SLS-backed integration
#    coverage is added in a follow-up commit.
class test_disagg_wt_page(wttest.WiredTigerTestCase):
    def _wt_bin(self):
        build = self.buildDirectory()
        libs_wt = os.path.join(build, '.libs', 'wt')
        if os.path.isfile(libs_wt):
            return libs_wt
        return os.path.join(build, 'wt')

    def test_page_subcommand_help(self):
        """`wt page -?` should be plumbed in: exit 0, print usage to stderr."""
        out = subprocess.run(
            [self._wt_bin(), 'page', '-?'],
            capture_output=True, text=True, check=False)
        self.assertEqual(out.returncode, 0, msg=out.stderr)
        self.assertIn('page -p page_id', out.stderr)

if __name__ == '__main__':
    wttest.run()
