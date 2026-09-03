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

import re, struct
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wtscenario import make_scenarios

# Offsets of the block header fields within a block, and the first byte past both headers. The
# on-disk byte order is little-endian regardless of the host.
DISK_SIZE_OFFSET = 28
CHECKSUM_OFFSET = 32
PAYLOAD_OFFSET = 40

# A block that fails its checksum is reported along with the fields decoded from the header it just
# read. Whether the size the block claims for itself agrees with the size that was asked for
# separates damage to the expected block from a different block arriving in its place, and the two
# need entirely different remediation.
@wttest.skip_for_hook("tiered", "corrupts local block files not used by tiered storage")
@wttest.skip_for_hook("disagg", "corrupts blocks which are not relevant for disagg")
class test_corrupt03(wttest.WiredTigerTestCase, suite_subprocess):
    test_name = __qualname__
    uri = f'table:{test_name}'
    tablename = f'{test_name}.wt'
    conn_config = 'cache_size=50MB,debug_mode=(corruption_abort=false)'
    table_config = 'key_format=i,value_format=S,allocation_size=512B,leaf_page_max=4KB'
    num_kv = 5000

    scenarios = make_scenarios([
        ('payload', dict(corrupt='payload')),
        ('disk_size', dict(corrupt='disk_size')),
        ('checksum_bit', dict(corrupt='checksum_bit')),
    ])

    def leaf_block(self):
        """
        Return the on-disk (offset, size) of a leaf block, taken from the addresses the wt utility
        prints for the checkpoint.
        """
        dump_file = 'dump_output.txt'
        self.runWt(['verify', '-d', 'dump_address', self.uri], outfilename=dump_file,
            closeconn=False)

        with open(dump_file, 'r', encoding='utf-8') as f:
            for line in f:
                if 'row-store leaf' not in line:
                    continue
                # An address looks like "[0: 708608-737280, 28672, 2171724032]".
                match = re.search(r'\[\d+: (\d+)-\d+, (\d+), \d+\]', line)
                if match:
                    return int(match.group(1)), int(match.group(2))

        self.fail("no leaf block address found in the dump output")

    def corrupt_block(self, offset, size):
        """
        Damage a leaf block in the way this scenario calls for.
        """
        with open(self.tablename, 'r+b') as f:
            if self.corrupt == 'payload':
                # Past both headers, but inside the first 64 bytes, which the checksum always
                # covers whether or not it extends over the whole block.
                f.seek(offset + PAYLOAD_OFFSET)
                original = f.read(8)
                f.seek(offset + PAYLOAD_OFFSET)
                f.write(bytes(b ^ 0xff for b in original))
            elif self.corrupt == 'disk_size':
                # Leave the block otherwise intact, but have it claim a size the address cookie
                # does not: that is what a valid block from a different offset looks like.
                f.seek(offset + DISK_SIZE_OFFSET)
                f.write(struct.pack('<I', size * 2))
            else:
                # A single bit in the stored checksum, which no scan of the block can find.
                f.seek(offset + CHECKSUM_OFFSET)
                original = f.read(1)
                f.seek(offset + CHECKSUM_OFFSET)
                f.write(bytes([original[0] ^ 0x04]))

    def expected_messages(self, size):
        if self.corrupt == 'payload':
            return [
                'calculated block checksum',
                'HEADER_SIZE_MATCH',
                f'disk_size {size}',
                f'requested size {size}',
            ]
        if self.corrupt == 'disk_size':
            return [
                'HEADER_SIZE_MISMATCH',
                f'disk_size {size * 2}',
                f'requested size {size}',
            ]
        return [
            'block header checksum',
            'HEADER_SIZE_MATCH',
            'single-bit flip detected in the stored block header checksum',
        ]

    def test_corrupt03(self):
        self.session.create(self.uri, self.table_config)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.num_kv):
            cursor[i] = 'a' * 100
        cursor.close()
        self.session.checkpoint()
        self.conn.close()

        offset, size = self.leaf_block()
        self.corrupt_block(offset, size)

        corrupt_conn = None
        try:
            corrupt_conn = self.setUpConnectionOpen('.')
            session = self.setUpSessionOpen(corrupt_conn)
            cursor = session.open_cursor(self.uri)
            while cursor.next() == 0:
                continue
            self.fail("reading the corrupt block should have failed")
        except wiredtiger.WiredTigerError:
            pass
        finally:
            if corrupt_conn is not None:
                self.assertRaises(
                    wiredtiger.WiredTigerError, lambda: corrupt_conn.close())

        stderr = self.readStderr(maxchars=1000000)
        for message in self.expected_messages(size):
            self.assertTrue(message in stderr,
                f'"{message}" missing from the corrupt-block report')

        self.ignoreStdoutPatternIfExists('extent list')
        self.ignoreStderrPatternIfExists('checksum error')
