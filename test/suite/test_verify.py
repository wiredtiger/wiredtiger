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

import os, re, struct
from suite_subprocess import suite_subprocess
import wiredtiger, wttest

# test_verify.py
#    Utilities: wt verify
class test_verify(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_verify.a'
    nentries = 1000
    outfile = "test_verify.out"
    errfile = "test_verify.err"
    params = 'key_format=S,value_format=S'

    # Returns the .wt file extension, or in the case
    # of tiered storage, builds the .wtobj object name.
    # Assumes that no checkpoints are done, so we
    # are on the first object.
    def file_name(self, name):
        return self.initialFileName('table:' + name)

    def populate(self, tablename, base = 0):
        """
        Insert some simple entries into the table
        """
        cursor = self.session.open_cursor('table:' + tablename, None, None)
        key = ''
        for i in range(base, base + self.nentries):
            key += str(i)
            cursor[key] = key + key
        cursor.close()

    def check_populate(self, tablename):
        """
        Verify that items added by populate are still there
        """
        cursor = self.session.open_cursor('table:' + tablename, None, None)
        wantkey = ''
        i = 0
        for gotkey, gotval in cursor:
            wantkey += str(i)
            wantval = wantkey + wantkey
            self.assertEqual(gotkey, wantkey)
            self.assertEqual(gotval, wantval)
            i += 1
        self.assertEqual(i, self.nentries)
        cursor.close()

    def count_file_contains(self, filename, content):
        count = 0
        with open(filename) as f:
            for line in f:
                if content in line:
                    count += 1
            f.close()
        return count

    def open_and_position(self, tablename, pct):
        """
        Open the file for the table, position it at a 4K page
        at roughly the given percentage into the file.
        As a side effect, the connection is closed.
        """
        # we close the connection to guarantee everything is
        # flushed and closed from the WT point of view.
        if self.conn != None:
            self.conn.close()
            self.conn = None
        filename = self.file_name(tablename)

        filesize = os.path.getsize(filename)
        position = int((filesize * pct) // 100)

        self.pr('damaging file at: ' + str(position))
        fp = open(filename, "r+b")
        fp.seek(position)
        return fp

    def open_and_offset(self, tablename, offset):
        """
        Open the file for the table, position it at the given offset.
        As a side effect, the connection is closed.
        """
        # we close the connection to guarantee everything is
        # flushed and closed from the WT point of view.
        if self.conn != None:
            self.conn.close()
            self.conn = None
        filename = self.file_name(tablename)

        self.pr('damaging file at: ' + str(offset))
        fp = open(filename, "r+b")
        fp.seek(offset)
        return fp

    def extract_checkpoint_names(self, tablename):
        metafile = "metadata.out"
        self.runWt(["list", "-v"], outfilename=metafile)

        with open(metafile, "r") as f:
            metadata = f.read()

        # Escape the filename for use in regex
        filename = "file:" + tablename + ".wt"
        # Capture everything after filename until the next section
        pattern = rf"{re.escape(filename)}\n(.*?)(?=\n\S|$)"
        match = re.search(pattern, metadata, re.DOTALL)

        if not match:
            return []

        section = match.group(1)

        # Find the "checkpoint=(...)" content
        checkpoint_match = re.search(r"checkpoint=\((.*?)\)", section, re.DOTALL)
        if not checkpoint_match:
            return []

        checkpoint_block = checkpoint_match.group(1)

        # Extract all checkpoint names, which are before '='
        checkpoint_names = re.findall(r'([\w\.]+)=\(', checkpoint_block)
        return checkpoint_names

    def test_verify_process_empty(self):
        """
        Test verify in a 'wt' process, using an empty table
        """
        self.session.create('table:' + self.tablename, self.params)
        # Run verify with an empty table
        self.runWt(["-v", "verify", "table:" + self.tablename], outfilename=self.outfile)
        self.assertEqual(self.count_file_contains(self.outfile,
            "table:" + self.tablename + " - done"), 1)

    def test_verify_process(self):
        """
        Test verify in a 'wt' process, using a populated table.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        self.runWt(["-v", "verify", "table:" + self.tablename], outfilename=self.outfile)
        self.assertEqual(self.count_file_contains(self.outfile,
            "table:" + self.tablename + " - done"), 1)

    def test_verify_api_empty(self):
        """
        Test verify via API, using an empty table
        """
        self.session.create('table:' + self.tablename, self.params)
        self.session.verify('table:' + self.tablename, None)

    def test_verify_api(self):
        """
        Test verify via API, using a populated table.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        self.verifyUntilSuccess(self.session, 'table:' + self.tablename)
        self.check_populate(self.tablename)

    def test_verify_api_75pct_null(self):
        """
        Test verify via API, on a damaged table.
        This is our only 'negative' test for verify using the API,
        it's uncertain that we can have reliable tests for this.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 75) as f:
            for i in range(0, 4096):
                f.write(struct.pack('B', 0))

        # open_and_position closed the session/connection, reopen them now.
        self.conn = self.setUpConnectionOpen(".")
        self.session = self.setUpSessionOpen(self.conn)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.verify('table:' + self.tablename, "read_corrupt"),
            "/WT_SESSION.verify/")
        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)
        self.assertGreaterEqual(self.count_file_contains("stderr.txt", "read checksum error"), 1)

    def test_verify_api_read_corrupt_pages(self):
        """
        Test verify via API, on a table that is purposely corrupted in
        multiple places. A verify operation with read_corrupt on should
        result in multiple checksum errors being logged.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 25) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        with self.open_and_position(self.tablename, 50) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        with self.open_and_position(self.tablename, 75) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)

    def test_verify_api_corrupt_first_page(self):
        """
        Test that verify works when the first child of an internal node is corrupted. A verify
        operation with read_corrupt on should result in a checksum errors being logged.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)

        # wt verify -d dump_address performs a depth-first traversal of the BTree. So the first
        # leaf page it prints is the first child of its parent. Grab the offset of this one so we
        # can corrupt it.
        self.runWt(['verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump.out')

        # Grab the offset position of the first page.
        offset = 0
        lines = open('dump.out').readlines()
        for line in lines:
            m = re.search(r'(\d+)-(\d+).*row-store leaf', line)
            if m:
                offset = int((int(m.group(2)) - int(m.group(1)))/2)
                break

        # Open the file and corrupt the first page.
        with self.open_and_offset(self.tablename, offset) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')

        # open_and_position closed the session/connection, reopen them now.
        self.conn = self.setUpConnectionOpen(".")
        self.session = self.setUpSessionOpen(self.conn)
        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.verify('table:' + self.tablename, "read_corrupt"),
            "/WT_SESSION.verify/")

        self.assertEqual(self.count_file_contains("stderr.txt",
            "calculated block checksum of"), 1)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)

    def test_verify_process_75pct_null(self):
        """
        Test verify in a 'wt' process on a table that is purposely damaged,
        with nulls at a position about 75% through.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 75) as f:
            for i in range(0, 4096):
                f.write(struct.pack('B', 0))
        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)
        self.runWt(["-p", "verify", "-c", "table:" + self.tablename],
            errfilename="verifyerr.out", failure=True)
        self.check_non_empty_file("verifyerr.out")
        self.assertGreaterEqual(self.count_file_contains("verifyerr.out", "read checksum error"), 1)

    def test_verify_process_25pct_junk(self):
        """
        Test verify in a 'wt' process on a table that is purposely damaged,
        with junk at a position about 25% through.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 25) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)
        self.runWt(["-p", "verify", "-c", "table:" + self.tablename],
            errfilename="verifyerr.out", failure=True)
        self.check_non_empty_file("verifyerr.out")
        self.assertGreaterEqual(self.count_file_contains("verifyerr.out", "read checksum error"), 1)

    def test_verify_process_read_corrupt_pages(self):
        """
        Test verify in a 'wt' process on a table that is purposely corrupted
        in multiple places. A verify operation with read_corrupt on should
        result in multiple checksum errors being logged.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 25) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        with self.open_and_position(self.tablename, 75) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        with self.open_and_position(self.tablename, 80) as f:
            for i in range(0, 100):
                f.write(b'\x01\xff\x80')
        self.runWt(["-p", "verify", "-c", "table:" + self.tablename],
            errfilename="verifyerr.out", failure=True)

        self.runWt(['-p', 'verify', '-d', 'dump_address', 'table:' + self.tablename, '-d'],
            outfilename='dump_corrupt.out', errfilename="dump_corrupt.err", failure=True)
        self.assertEqual(self.count_file_contains("dump_corrupt.out",
            "Read failure while accessing a page from the "), 1)
        self.check_non_empty_file("verifyerr.out")

        # It is expected that more than one checksum error is logged given
        # that we have corrupted the table in multiple locations, but we may
        # not necessarily detect all three corruptions - e.g. we won't detect
        # a corruption if we overwrite free space or overwrite a page that is
        # a child of another page that we overwrite.
        self.assertGreaterEqual(self.count_file_contains("verifyerr.out", "read checksum error"), 1)

    def test_verify_process_truncated(self):
        """
        Test verify in a 'wt' process on a table that is purposely damaged,
        truncated about 75% through.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 75) as f:
            f.truncate(0)
        self.runWt(["-p", "verify", "table:" + self.tablename],
            errfilename="verifyerr.out", failure=True)
        # The test may output the following error message while opening a file that
        # does not exist. Ignore that.
        self.ignoreStderrPatternIfExists('No such file or directory')

    def test_verify_process_zero_length(self):
        """
        Test verify in a 'wt' process on a zero-length table.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        with self.open_and_position(self.tablename, 0) as f:
            f.truncate(0)
        self.runWt(["verify", "table:" + self.tablename],
            errfilename="verifyerr.out", failure=True)
        # The test may output the following error message while opening a file that
        # does not exist. Ignore that.
        self.ignoreStderrPatternIfExists('No such file or directory')

    def test_verify_all(self):
        """
        Test verify in a 'wt' process without a specific table URI argument.
        """
        ntables = 3

        for i in range(ntables):
            self.session.create('table:' + self.tablename + str(i), self.params)
            self.populate(self.tablename + str(i))
        self.session.checkpoint()

        self.runWt(["-v", "verify"], outfilename=self.outfile)
        for i in range(ntables):
            self.assertEqual(self.count_file_contains(self.outfile,
                "table:" +  self.tablename + str(i) + " - done"), 1)

        # Purposely corrupt the last two tables. Test that verifying the database
        # with the abort option stops after seeing the first corrupted table.
        for i in range(1, ntables):
            with self.open_and_position(self.tablename + str(i), 75) as f:
                for i in range(0, 4096):
                    f.write(struct.pack('B', 0))

        self.runWt(["-v", "-p", "verify", "-a"], outfilename=self.outfile,
            errfilename="verifyerr.out", failure=True)

        self.assertEqual(self.count_file_contains(self.outfile,
            "table:" + self.tablename + "0" + " - done"), 1)
        self.assertEqual(self.count_file_contains("verifyerr.out",
            "table:" + self.tablename + "1" + ": WT_ERROR"), 1)
        self.assertEqual(self.count_file_contains("verifyerr.out",
            "table:" + self.tablename + "2" + ": WT_ERROR"), 0)

    def test_verify_ckpt(self):
        """
        Test verify in a 'wt' process with an option for a specific checkpoint verification.
        Custom checkpoint verification.
        """
        tables = [self.tablename + str(i) for i in range(0, 3)]
        ckpt = 'CustomCkpt'
        next_ckpt = 'NextCkpt'

        for table in tables[:-1]:
            self.session.create('table:' + table, self.params)
            self.populate(table)

        self.session.checkpoint("name=" + ckpt)

        self.session.create('table:' + tables[-1], self.params)
        self.populate(tables[-1])
        # Insert some non-checkpointed entries to the first table
        self.populate(tables[0], self.nentries)

        self.session.checkpoint("name=" + next_ckpt) # Uncomment later, create checkpoint to be sure that data populated later is checkpointed too

        self.runWt(["-v", "verify", "-C", ckpt], outfilename=self.outfile)

        # The last table shouldn't be verified since it doesn't contain the specified checkpoint
        for tablename in tables[:-1]:
            self.assertEqual(self.count_file_contains(self.outfile,
                "table:" + tablename + " - done") , 1)
        self.check_file_not_contains(self.outfile, "table:" + tables[-1] + " - done")

        # This checkpoint cover all tables
        self.runWt(["-v", "verify", "-C", next_ckpt], outfilename=self.outfile)
        for tablename in tables:
            self.assertEqual(self.count_file_contains(self.outfile,
                "table:" + tablename + " - done") , 1)

    def test_verify_missing_ckpt(self):
        """
        Test verify in a 'wt' process with an option for a specific checkpoint verification.
        Nonexistent checkpoint verification
        """

        def verify_missing_ckpts():
            ckpts = ["NonexistentCkpt", "WiredTigerCheckpoint.abc", "WiredTigerCheckpoint.99999"]
            for ckpt in ckpts:
                self.runWt(["verify", "-C", ckpt], errfilename=self.errfile, failure=True)
                self.check_file_content(self.errfile, "wt: session.verify: No such file or directory\n")

        verify_missing_ckpts()
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)
        verify_missing_ckpts()

    def test_verify_wt_ckpt(self):
        """
        Test verify in a 'wt' process with an option for a specific checkpoint verification.
        WiredTiger checkpoint verification.
        """
        self.session.create('table:' + self.tablename, self.params)
        self.populate(self.tablename)

        # Parse internal checkpoints from metadata
        unnamed_ckpts = self.extract_checkpoint_names(self.tablename)
        self.assertNotEqual([], unnamed_ckpts)

        for ckpt in unnamed_ckpts:
            self.runWt(["-v", "verify", "-C", ckpt], outfilename=self.outfile)
            self.assertEqual(self.count_file_contains(self.outfile,
                "table:" + self.tablename + " - done"), 1)
