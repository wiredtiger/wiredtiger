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
#

import datetime, inspect, os, random, wiredtiger

# These routines help run the various page log sources used by disaggregated storage.
# They are required to manage the generation of disaggregated storage specific configurations.

# Set up configuration
def get_conn_config(disagg_storage):
    if not disagg_storage.is_disagg_scenario():
            return ''
    if disagg_storage.ds_name == 'palm' and not os.path.exists(disagg_storage.bucket):
            os.mkdir(disagg_storage.bucket)
    return \
        f'statistics=(all),name={disagg_storage.ds_name},'

def gen_disagg_storages(test_name='', disagg_only = False):
    disagg_storages = [
        ('palm', dict(is_disagg = True,
            is_local_storage = True,
            num_ops=100,
            ds_name = 'palm')),
        # This must be the last item as we separate the non-disagg from the disagg items later on.
        ('non_disagg', dict(is_disagg = False)),
    ]

    if disagg_only:
        return disagg_storages[:-1]

    return disagg_storages

# This mixin class provides disaggregated storage configuration methods.
class DisaggConfigMixin:
    # Returns True if the current scenario is disaggregated.
    def is_disagg_scenario(self):
        return hasattr(self, 'is_disagg') and self.is_disagg

    # Setup connection config.
    def conn_config(self):
        return self.disagg_conn_config()

    # Can be overridden
    def additional_conn_config(self):
        return ''

    # Setup disaggregated connection config.
    def disagg_conn_config(self):
        # Handle non_disaggregated storage scenarios.
        if not self.is_disagg_scenario():
            return self.additional_conn_config()

        # Setup directories structure for local disaggregated storage.
        if self.is_local_storage:
            bucket_full_path = os.path.join(self.home, self.bucket)
            if not os.path.exists(bucket_full_path):
                os.mkdir(bucket_full_path)

        # Build disaggregated storage connection string.
        # Any additional configuration appears first to override this configuration.
        return \
            self.additional_conn_config() + f'name={self.ds_name}),'

    # Load the storage sources extension.
    def conn_extensions(self, extlist):
        return self.disagg_conn_extensions(extlist)

    # Returns configuration to be passed to the extension.
    # Call may override, in which case, they probably want to
    # look at self.is_local_storage or self.ds_name, as every
    # extension has their own configurations that are valid.
    #
    # Some possible values to return: 'verbose=1'
    # or for palm: 'verbose=1,delay_ms=13,force_delay=30'
    # or 'materialization_delay_ms=1000'
    def disaggregated_extension_config(self):
        return 'verbose=1'

    # Load disaggregated storage extension.
    def disagg_conn_extensions(self, extlist):
        # Handle non_disaggregated storage scenarios.
        if not self.is_disagg_scenario():
            return ''

        config = self.disaggregated_extension_config()
        if config == None:
            config = ''
        elif config != '':
            config = f'=(config=\"({config})\")'

        # S3 store is built as an optional loadable extension, not all test environments build S3.
        if not self.is_local_storage:
            extlist.skip_if_missing = True
        # Windows doesn't support dynamically loaded extension libraries.
        if os.name == 'nt':
            extlist.skip_if_missing = True
        extlist.extension('page_log', self.ds_name + config)

    # Get the last completed checkpoint
    def disagg_get_complete_checkpoint(self, conn=None):
        if conn is None:
            conn = self.conn
        page_log = conn.get_page_log('palm')

        session = conn.open_session('')
        (ret, n) = page_log.pl_get_complete_checkpoint(session)
        session.close()

        self.assertEquals(ret, 0)
        return n

    # Get the currently open checkpoint
    def disagg_get_open_checkpoint(self, conn=None):
        if conn is None:
            conn = self.conn
        page_log = conn.get_page_log('palm')

        session = conn.open_session('')
        (ret, n) = page_log.pl_get_open_checkpoint(session)
        session.close()

        self.assertEquals(ret, 0)
        return n

    # Let the follower pick up the latest checkpoint
    def disagg_advance_checkpoint(self, conn_follower, conn_leader=None):
        n = self.disagg_get_complete_checkpoint(conn_leader)
        self.assertGreater(n, 0)
        conn_follower.reconfigure(f'disaggregated=(checkpoint_id={n})')

    # Switch the leader and the follower
    def disagg_switch_follower_and_leader(self, conn_follower, conn_leader=None):
        if conn_leader is None:
            conn_leader = self.conn

        # Leader step down
        conn_leader.reconfigure(f'disaggregated=(role="follower")')

        complete_id = self.disagg_get_complete_checkpoint(conn_leader)
        self.assertGreater(complete_id, 0)
        open_id = self.disagg_get_open_checkpoint(conn_leader)
        if open_id == 0:
            next_id = complete_id + 1
        else:
            self.assertGreaterEqual(open_id, complete_id)
            next_id = open_id + 1

        # Follower step up, including picking up the last complete checkpoint
        conn_follower.reconfigure(f'disaggregated=(checkpoint_id={complete_id},' +\
                                  f'next_checkpoint_id={next_id},role="leader")')
