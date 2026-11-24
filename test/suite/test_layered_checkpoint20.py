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

import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

def encode_bytes(str):
    return bytes(str, 'utf-8')

# The extended abandon call drops the records written after the given checkpoint, which must be the
# last completed one, and refuses a call without a boundary. These are internal APIs, tested
# independently of WiredTiger's own use of them.
class test_layered_checkpoint20(wttest.WiredTigerTestCase, DisaggConfigMixin):

    disagg_storages = gen_disagg_storages(disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def checkpoint(self, page_log, session, checkpoint_id):
        args = wiredtiger.PageLogCompleteCheckpointArgs()
        args.checkpoint_id = checkpoint_id
        args.checkpoint_timestamp = checkpoint_id
        args.checkpoint_metadata = f'Checkpoint{checkpoint_id}'
        args.checkpoint_oldest_timestamp = 0
        args.lsn = 0
        page_log.pl_complete_checkpoint(session, args)
        return args.lsn

    def put_page(self, handle, session, page_id, payload):
        args = wiredtiger.PageLogPutArgs()
        args.flags = 0
        args.base_lsn = 0
        args.backlink_lsn = 0
        handle.plh_put(session, page_id, 0, args, encode_bytes(payload))
        return args.lsn

    def abandon(self, page_log, session, last_checkpoint_lsn):
        args = wiredtiger.PageLogAbandonCheckpointArgs()
        args.last_checkpoint_lsn = last_checkpoint_lsn
        page_log.pl_abandon_checkpoint_ext(session, args)

    def test_abandon_discards_records_after_checkpoint(self):
        session = self.session
        page_log = self.conn.get_page_log('palite')
        handle = page_log.pl_open_handle(session, 1)

        # Page 10 exists before the checkpoint whose completion LSN is the abandon boundary.
        self.put_page(handle, session, 10, 'page-10')
        ckpt1_lsn = self.checkpoint(page_log, session, 1)

        # Pages 11 and 12 belong to an incomplete checkpoint: they are written after the completed
        # one, with no completion of their own.
        self.put_page(handle, session, 11, 'page-11')
        page12_lsn = self.put_page(handle, session, 12, 'page-12')
        self.assertGreater(page12_lsn, ckpt1_lsn)
        self.assertEqual(sorted(handle.plh_get_page_ids(session, page12_lsn)), [10, 11, 12])

        # Abandoning at the completed checkpoint discards the records written after it and keeps
        # the checkpoint itself.
        self.abandon(page_log, session, ckpt1_lsn)
        self.assertEqual(page_log.pl_get_complete_checkpoint(session)[0], ckpt1_lsn)
        self.assertEqual(sorted(handle.plh_get_page_ids(session, page12_lsn)), [10])

        # Page 13 belongs to the next checkpoint, page 14 to the incomplete one after it.
        page13_lsn = self.put_page(handle, session, 13, 'page-13')
        ckpt2_lsn = self.checkpoint(page_log, session, 2)
        page14_lsn = self.put_page(handle, session, 14, 'page-14')
        self.assertGreater(ckpt2_lsn, page13_lsn)
        self.assertGreater(page14_lsn, ckpt2_lsn)
        self.assertEqual(sorted(handle.plh_get_page_ids(session, page14_lsn)), [10, 13, 14])
        self.abandon(page_log, session, ckpt2_lsn)
        self.assertEqual(page_log.pl_get_complete_checkpoint(session)[0], ckpt2_lsn)
        self.assertEqual(sorted(handle.plh_get_page_ids(session, page14_lsn)), [10, 13])

    def test_abandon_requires_a_boundary(self):
        session = self.session
        page_log = self.conn.get_page_log('palite')

        # Without a boundary there is nothing safe to discard.
        self.put_page(page_log.pl_open_handle(session, 1), session, 20, 'page-20')
        self.checkpoint(page_log, session, 1)
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: self.abandon(page_log, session, 0), '/Invalid argument/')

if __name__ == '__main__':
    wttest.run()
