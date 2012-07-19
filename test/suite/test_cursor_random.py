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
#
# test_cursor01.py
# 	Cursor operations
#

import wiredtiger, wttest

class Test_Cursor_Random(wttest.WiredTigerTestCase):
    """
    Test basic operations
    """
    table_name1 = 'test_cursor_random'
    nentries = 10

    scenarios = [
        ('row', dict(tablekind='row')),
        ('col', dict(tablekind='col')),
        ('fix', dict(tablekind='fix'))
        ]

    def config_string(self):
        """
        Return any additional configuration.
        This method may be overridden.
        """
        return ""

    def session_create(self, name, args):
        """
        session.create, but report errors more completely
        """
        try:
            self.session.create(name, args)
        except:
            print('**** ERROR in session.create("' + name + '","' + args + '") ***** ')
            raise

    def create_session_and_cursor(self,randomize=False):
        tablearg = "table:" + self.table_name1
        if self.tablekind == 'row':
            keyformat = 'key_format=S'
        else:
            keyformat = 'key_format=r'  # record format
        if self.tablekind == 'fix':
            valformat = 'value_format=8t'
        else:
            valformat = 'value_format=S'
        create_args = keyformat + ',' + valformat + self.config_string()
        #print 'Args= %s' % create_args
        self.pr('creating session: ' + create_args)
        self.session_create(tablearg, create_args)
        self.pr('creating cursor')
        config = None
        if randomize:
            config = "next_random=true"
        return self.session.open_cursor(tablearg, None, config)

    def genkey(self, i):
        if self.tablekind == 'row':
            return 'key' + str(i)
        else:
            return long(i+1)

    def genvalue(self, i):
        if self.tablekind == 'fix':
            return int(i & 0xff)
        else:
            return 'value' + str(i)

    def test_cursor_random(self):
        cursor = self.create_session_and_cursor()
        for i in range(0, self.nentries):
            cursor.set_key(self.genkey(i))
            cursor.set_value(self.genvalue(i))
            cursor.insert()
        cursor.close()
        if self.tablekind == 'row':
            cursor = self.create_session_and_cursor(randomize=True)
            self.assertEqual(cursor.next(), 0)
            value = str(cursor.get_value())
            print "Value= %s" % value
            self.assertTrue(value in ['value'+str(x) for x in range(0, self.nentries)])
            cursor.close()
        else:#becasue next_random only works in row format, otherwise throws exceptino
            cursor = self.create_session_and_cursor(randomize=True)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                    lambda: cursor.next(),
                    r"")
            cursor.close()
    def test_cursor_random_empty_table(self):
        if self.tablekind == "row":
            cursor = self.create_session_and_cursor(randomize=True)
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda: cursor.next(),
                r"")

