#!/usr/bin/env python
#
# Copyright (c) 2008-2012 WiredTiger, Inc.
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
from helper import key_populate, simple_populate

# test_checkpoint01.py
#    Checkpoint tests
# General checkpoint test: create an object containing sets of data associated
# with a set of checkpoints, then confirm the checkpoint's values are correct,
# including after other checkpoints are dropped.
class test_checkpoint(wttest.WiredTigerTestCase):
    scenarios = [
        ('file', dict(uri='file:checkpoint',fmt='S')),
        ('table', dict(uri='table:checkpoint',fmt='S'))
        ]

    # Each checkpoint has a key range and a "is dropped" flag.
    checkpoints = {
        "checkpoint-1": ((100, 200), 0),
        "checkpoint-2": ((200, 220), 0),
        "checkpoint-3": ((300, 320), 0),
        "checkpoint-4": ((400, 420), 0),
        "checkpoint-5": ((500, 520), 0),
        "checkpoint-6": ((100, 620), 0),
        "checkpoint-7": ((200, 250), 0),
        "checkpoint-8": ((300, 820), 0),
        "checkpoint-9": ((400, 920), 0)
        }

    # Add a set of records for a checkpoint.
    def add_records(self, name):
        cursor = self.session.open_cursor(self.uri, None, "overwrite")
        start, stop = self.checkpoints[name][0]
        for i in range(start, stop+1):
            cursor.set_key("%010d KEY------" % i)
            cursor.set_value("%010d VALUE "% i + name)
            self.assertEqual(cursor.insert(), 0)
        cursor.close()
        self.checkpoints[name] = (self.checkpoints[name][0], 1)

    # For each checkpoint entry, add/overwrite the specified records, then
    # checkpoint the object, and verify it (which verifies all underlying
    # checkpoints individually).
    def build_file_with_checkpoints(self):
        for checkpoint_name, entry in self.checkpoints.iteritems():
            self.add_records(checkpoint_name)
            self.session.checkpoint("name=" + checkpoint_name)
            
    # Create a dictionary of sorted records a checkpoint should include.
    def list_expected(self, name):
        records = {}
        for checkpoint_name, entry in self.checkpoints.iteritems():
            start, stop = entry[0]
            for i in range(start, stop+1):
                records['%010d KEY------' % i] =\
                    '%010d VALUE ' % i + checkpoint_name
            if name == checkpoint_name:
                break
        return records

    # Create a dictionary of sorted records a checkpoint does include.
    def list_checkpoint(self, name):
        records = {}
        cursor = self.session.open_cursor(self.uri, None, 'checkpoint=' + name)
        while cursor.next() == 0:
            records[cursor.get_key()] = cursor.get_value()
        cursor.close()
        return records

    # For each existing checkpoint entry, verify it contains the records it
    # should, and no other checkpoints exist.
    def check(self):
        # Physically verify the file, including the individual checkpoints.
        self.session.verify(self.uri, None)

        for checkpoint_name, entry in self.checkpoints.iteritems():
            if entry[1] == 0:
                self.assertRaises(wiredtiger.WiredTigerError,
                    lambda: self.session.open_cursor(
                    self.uri, None, "checkpoint=" + checkpoint_name))
            else:
                list_expected = self.list_expected(checkpoint_name)
                list_checkpoint = self.list_checkpoint(checkpoint_name)
                self.assertEqual(list_expected, list_checkpoint)

    # Main checkpoint test driver.
    def test_checkpoint(self):
        # Build a file with a set of checkpoints, and confirm they all have
        # the correct key/value pairs.
        self.session.create(self.uri,
            "key_format=" + self.fmt + ",value_format=S,leaf_page_max=512")
        self.build_file_with_checkpoints()
        self.check()

        # Drop a set of checkpoints sequentially, and each time confirm the
        # contents of remaining checkpoints, and that dropped checkpoints
        # don't appear.
        for i in [1,3,7,9]:
            checkpoint_name = 'checkpoint-' + str(i)
            self.session.checkpoint('drop=(' + checkpoint_name + ')')
            self.checkpoints[checkpoint_name] =\
                (self.checkpoints[checkpoint_name][0], 0)
            self.check()

        # Drop remaining checkpoints, all subsequent checkpoint opens should
        # fail.
        self.session.checkpoint("drop=(from=all)")
        for checkpoint_name, entry in self.checkpoints.iteritems():
            self.checkpoints[checkpoint_name] =\
                (self.checkpoints[checkpoint_name][0], 0)
        self.check()


# Check some specific cursor checkpoint combinations.
class test_checkpoint_cursor(wttest.WiredTigerTestCase):
    scenarios = [
        ('file', dict(uri='file:checkpoint',fmt='S')),
        ('table', dict(uri='table:checkpoint',fmt='S'))
        ]

    # Check that you cannot open a checkpoint that doesn't exist.
    def test_checkpoint_dne(self):
        simple_populate(self, self.uri, 'key_format=' + self.fmt, 100)
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(
            self.uri, None, "checkpoint=checkpoint-1"))
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(
            self.uri, None, "checkpoint=WiredTigerCheckpoint"))

    # Check that you can open checkpoints more than once.
    def test_checkpoint_multiple_open(self):
        simple_populate(self, self.uri, 'key_format=' + self.fmt, 100)
        self.session.checkpoint("name=checkpoint-1")
        c1 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-1")
        c2 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-1")
        c3 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-1")
        c4 = self.session.open_cursor(self.uri, None, None)
        c4.close, c3.close, c2.close, c1.close

        self.session.checkpoint("name=checkpoint-2")
        c1 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-1")
        c2 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-2")
        c3 = self.session.open_cursor(self.uri, None, "checkpoint=checkpoint-2")
        c4 = self.session.open_cursor(self.uri, None, None)
        c4.close, c3.close, c2.close, c1.close

    # Check that you cannot drop a checkpoint if it's in use.
    def test_checkpoint_inuse(self):
        simple_populate(self, self.uri, 'key_format=' + self.fmt, 100)
        self.session.checkpoint("name=checkpoint-1")
        self.session.checkpoint("name=checkpoint-2")
        self.session.checkpoint("name=checkpoint-3")
        cursor = self.session.open_cursor(
            self.uri, None, "checkpoint=checkpoint-2")

        # Check creating an identically named checkpoint fails. */
        # Check dropping the specific checkpoint fails.
        # Check dropping all checkpoints fails.
        msg = '/checkpoints cannot be dropped/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.checkpoint("name=checkpoint-2"), msg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.checkpoint("drop=(checkpoint-2)"), msg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.checkpoint("drop=(from=all)"), msg)

        # Check dropping other checkpoints succeeds (which also tests that you
        # can create new checkpoints while other checkpoints are in-use).
        self.session.checkpoint("drop=(checkpoint-1,checkpoint-3)")

        # Close the cursor and repeat the failing commands, they should now
        # succeed.
        cursor.close()
        self.session.checkpoint("name=checkpoint-2")
        self.session.checkpoint("drop=(checkpoint-2)")
        self.session.checkpoint("drop=(from=all)")


# Check that you can checkpoint targets.
class test_checkpoint_target(wttest.WiredTigerTestCase):
    scenarios = [
        ('file', dict(uri='file:checkpoint',fmt='S')),
        ('table', dict(uri='table:checkpoint',fmt='S'))
        ]

    def update(self, uri, value):
        cursor = self.session.open_cursor(uri, None, "overwrite")
        cursor.set_key(key_populate(self.fmt, 10))
        cursor.set_value(value)
        cursor.insert()
        cursor.close()

    def check(self, uri, value):
        cursor = self.session.open_cursor(uri, None, "checkpoint=checkpoint-1")
        cursor.set_key(key_populate(self.fmt, 10))
        cursor.search()
        self.assertEquals(cursor.get_value(), value)
        cursor.close()

    def test_checkpoint_target(self):
        # Create 3 objects, change one record to an easily recognizable string.
        uri = self.uri + '1'
        simple_populate(self, uri, 'key_format=' + self.fmt, 100)
        self.update(uri, 'ORIGINAL')
        uri = self.uri + '2'
        simple_populate(self, uri, 'key_format=' + self.fmt, 100)
        self.update(uri, 'ORIGINAL')
        uri = self.uri + '3'
        simple_populate(self, uri, 'key_format=' + self.fmt, 100)
        self.update(uri, 'ORIGINAL')

        # Checkpoint all three objects.
        self.session.checkpoint("name=checkpoint-1")

        # Update all 3 objects, then checkpoint two of the objects with the
        # same checkpoint name.
        self.update(self.uri + '1', 'UPDATE')
        self.update(self.uri + '2', 'UPDATE')
        self.update(self.uri + '3', 'UPDATE')
        target = 'target=("' + self.uri + '1"' + ',"' + self.uri + '2")'
        self.session.checkpoint("name=checkpoint-1," + target)

        # Confirm the checkpoint has the old value in objects that weren't
        # checkpointed, and the new value in objects that were checkpointed.
        self.check(self.uri + '1', 'UPDATE')
        self.check(self.uri + '2', 'UPDATE')
        self.check(self.uri + '3', 'ORIGINAL')


# Check that you can't write checkpoint cursors.
class test_checkpoint_cursor_update(wttest.WiredTigerTestCase):
    scenarios = [
        ('file', dict(uri='file:checkpoint',fmt='r')),
        ('file', dict(uri='file:checkpoint',fmt='S')),
        ('table', dict(uri='table:checkpoint',fmt='r')),
        ('table', dict(uri='table:checkpoint',fmt='S'))
        ]

    def test_checkpoint_cursor_update(self):
        simple_populate(self, self.uri, 'key_format=' + self.fmt, 100)
        self.session.checkpoint("name=ckpt")
        cursor = self.session.open_cursor(self.uri, None, "checkpoint=ckpt")
        cursor.set_key(key_populate(self.fmt, 10))
        cursor.set_value("XXX")
        self.assertRaises(wiredtiger.WiredTigerError, lambda: cursor.insert())
        self.assertRaises(wiredtiger.WiredTigerError, lambda: cursor.remove())
        self.assertRaises(wiredtiger.WiredTigerError, lambda: cursor.update())
        cursor.close()


# Check that WiredTigerCheckpoint works as a checkpoint specifier.
class test_checkpoint_last(wttest.WiredTigerTestCase):
    scenarios = [
        ('file', dict(uri='file:checkpoint',fmt='S')),
        #('table', dict(uri='table:checkpoint',fmt='S'))
        ]

    def test_checkpoint_last(self):
        # Create an object, change one record to an easily recognizable string,
        # then checkpoint it and open a cursor, confirming we see the correct
        # value.   Repeat this action, we want to be sure the engine gets the
        # latest checkpoint information each time.
        uri = self.uri
        simple_populate(self, uri, 'key_format=' + self.fmt, 100)

        for value in ('FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FIFTH'):
            # Update the object.
            cursor = self.session.open_cursor(uri, None, "overwrite")
            cursor.set_key(key_populate(self.fmt, 10))
            cursor.set_value(value)
            cursor.insert()
            cursor.close()

            # Checkpoint the object.
            self.session.checkpoint()

            # Verify the "last" checkpoint sees the correct value.
            cursor = self.session.open_cursor(
                uri, None, "checkpoint=WiredTigerCheckpoint")
            cursor.set_key(key_populate(self.fmt, 10))
            cursor.search()
            self.assertEquals(cursor.get_value(), value)
            # Don't close the checkpoint cursor, we want it to remain open until
            # the test completes.


# Check we can't use the reserved name as an application checkpoint name.
class test_checkpoint_last_name(wttest.WiredTigerTestCase):
    def test_checkpoint_last_name(self):
        simple_populate(self, "file:checkpoint", 'key_format=S', 100)
        msg = '/the checkpoint name.*is reserved/'
        for conf in (
            'name=WiredTigerCheckpoint',
            'name=WiredTigerCheckpoint.',
            'name=WiredTigerCheckpointX',
            'drop=(from=WiredTigerCheckpoint)',
            'drop=(from=WiredTigerCheckpoint.)',
            'drop=(from=WiredTigerCheckpointX)',
            'drop=(to=WiredTigerCheckpoint)',
            'drop=(to=WiredTigerCheckpoint.)',
            'drop=(to=WiredTigerCheckpointX)'):
                self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: self.session.checkpoint(conf), msg)


if __name__ == '__main__':
    wttest.run()
