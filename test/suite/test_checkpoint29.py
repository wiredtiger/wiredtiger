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
from wtscenario import make_scenarios

# test_checkpoint29.py
#
# Test opening a checkpoint cursor after bulk operations.
class test_checkpoint(wttest.WiredTigerTestCase):
    internal_checkpoint_name = 'WiredTigerCheckpoint'

    checkpoint_names = [
        ('no_named_checkpoint', dict(checkpoint_name='')),
        ('named_checkpoint', dict(checkpoint_name='checkpoint_1')),
    ]

    scenarios = make_scenarios(checkpoint_names)

    def test_checkpoint(self):
        uri = 'table:checkpoint29'

        # Create an empty table.
        self.session.create(uri, "key_format=S,value_format=S")

        # Ensure that everything is written to the disk.
        self.session.checkpoint()

        # Create a named checkpoint if requested.
        named_checkpoint_id = 0
        if self.checkpoint_name:
            self.session.checkpoint(f"name={self.checkpoint_name}")
            cursor = self.session.open_cursor(
                uri, None, f"checkpoint={self.checkpoint_name}")
            # Save the checkpoint ID for future comparison.
            named_checkpoint_id = cursor.checkpoint_id()
            cursor.close()

        # Open a bulk cursor on the table and close it. This will create a single-file checkpoint on
        # the table.
        cursor = self.session.open_cursor(uri, None, "bulk")
        cursor.close()

        # In both cases, try to open a checkpoint cursor using the internal WiredTiger checkpoint
        # name.
        # If no named checkpoint was created before the closure of the bulk cursor, only the
        # single-file checkpoint from the bulk cursor is visible. Since it cannot be used by the
        # checkpoint cursor, an error is returned.
        # If a named checkpoint was created, the checkpoint cursor should select that one.
        if not self.checkpoint_name:
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(
                    uri, None, f"checkpoint={self.internal_checkpoint_name}"))
        else:
            cursor = self.session.open_cursor(
                uri, None, f"checkpoint={self.internal_checkpoint_name}")
            assert named_checkpoint_id == cursor.checkpoint_id()
            cursor.close()

            # Opening a checkpoint cursor using the named checkpoint should not be an issue.
            cursor = self.session.open_cursor(uri, None, f"checkpoint={self.checkpoint_name}")
            assert named_checkpoint_id == cursor.checkpoint_id()
            cursor.close()

if __name__ == '__main__':
    wttest.run()
