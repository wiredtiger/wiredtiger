#!/usr/bin/env python
#
# Public Domain 2014-2017 MongoDB, Inc.
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

import sys, wiredtiger, wttest
from wtscenario import make_scenarios

# test_alter02.py
#    Smoke-test the session alter operations.
class test_alter02(wttest.WiredTigerTestCase):
    entries = 500
    conn_config = 'log=(archive=false,enabled,file_max=100K)'
    # Binary values.
    value = u'\u0001\u0002abcd\u0003\u0004'
    value2 = u'\u0001\u0002dcba\u0003\u0004'

    types = [
        ('file', dict(uri='file:', use_cg=False, use_index=False)),
        ('lsm', dict(uri='lsm:', use_cg=False, use_index=False)),
        ('table-cg', dict(uri='table:', use_cg=True, use_index=False)),
        ('table-index', dict(uri='table:', use_cg=False, use_index=True)),
        ('table-simple', dict(uri='table:', use_cg=False, use_index=False)),
    ]

    tables = [
        ('always-logged', dict(name='table0', logcreate=True, logalter=True)),
        ('create-logged', dict(name='table1', logcreate=True, logalter=False)),
        ('alter-logged', dict(name='table2', logcreate=False, logalter=True)),
        ('never-logged', dict(name='table3', logcreate=False, logalter=False)),
    ]

    reopen = [
        ('no-reopen', dict(reopen=False)),
        ('reopen', dict(reopen=True)),
    ]
    scenarios = make_scenarios(types, tables, reopen)

    # Verify the metadata string for this URI and that its setting in the
    # metdata file is correct.
    def verify_metadata(self, metastr):
        if metastr == '':
            return
        cursor = self.session.open_cursor('metadata:', None, None)
        #
        # Walk through all the metadata looking for the entries that are
        # the file URIs for components of the table.
        #
        found = False
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            key = cursor.get_key()
            check_meta = ((key.find("lsm:") != -1 or key.find("file:") != -1) \
                and key.find(self.name) != -1)
            if check_meta:
                value = cursor[key]
                found = True
                self.assertTrue(value.find(metastr) != -1)
        cursor.close()
        self.assertTrue(found == True)

    # Verify the data in the log.  If the data should be logged we write one
    # value.  If it should not be logged, we write a different value.
    def verify_logrecs(self, expected_keys):
        c = self.session.open_cursor('log:', None, None)
        count = 0
        while c.next() == 0:
            # lsn.file, lsn.offset, opcount
            keys = c.get_key()
            # txnid, rectype, optype, fileid, logrec_key, logrec_value
            values = c.get_value()
            # !!! rectype 1 == WT_LOGREC_COMMIT
            try:
                if self.value in str(values[5]):     # logrec_value
                    count += 1
                self.assertFalse(value2 in str(values[5]))
            except:
                pass
        c.close()
        self.assertEqual(count, expected_keys)

    # Alter: Change the log setting after creation
    def test_alter02_log(self):
        uri = self.uri + self.name
        create_params = 'key_format=i,value_format=S,'
        complex_params = ''

        #
        # If we're not explicitly setting the parameter, then don't
        # modify create_params to test using the default.
        #
        if self.logcreate:
            log_param = 'log=(enabled=true)'
        else:
            log_param = 'log=(enabled=false)'
        create_params += '%s,' % log_param
        complex_params += '%s,' % log_param

        cgparam = ''
        if self.use_cg or self.use_index:
            cgparam = 'columns=(k,v),'
        if self.use_cg:
            cgparam += 'colgroups=(g0),'

        self.pr("CREATE: create_params " + create_params + cgparam)
        self.session.create(uri, create_params + cgparam)
        # Add in column group or index settings.
        if self.use_cg:
            cgparam = 'columns=(v),'
            suburi = 'colgroup:' + self.name + ':g0'
            self.session.create(suburi, complex_params + cgparam)
        if self.use_index:
            suburi = 'index:' + self.name + ':i0'
            self.session.create(suburi, complex_params + cgparam)

        # Put some data in table.
        c = self.session.open_cursor(uri, None)
        if self.logcreate:
            myvalue = self.value
        else:
            myvalue = self.value2
        for k in range(self.entries):
            c[k] = myvalue
        c.close()

        # Verify the string in the metadata
        self.verify_metadata(log_param)

        # Verify the logged operations
        if self.logcreate:
            expected_keys = self.entries
        else:
            expected_keys = 0
        self.verify_logrecs(expected_keys)

        # Run through all combinations of the alter commands
        # for all allowed settings.  This tests having only one or
        # the other set as well as having both set.  It will also
        # cover trying to change the setting to its current value.
        alter_param = ''
        if self.logalter:
            log_str = 'log=(enabled=true)'
        else:
            log_str = 'log=(enabled=false)'
        alter_param = '%s' % log_str
        special = self.use_cg or self.use_index

        # Altering the log setting currently fails on an
        # open handle, so it is expected to succeed when
        # we reopen the connection and return an error when
        # we attempt with the same open connection.
        if self.reopen:
            self.reopen_conn()
            self.session.alter(uri, alter_param)
            if special:
                self.session.alter(suburi, alter_param)
                self.verify_metadata(log_str)
            else:
                self.verify_metadata(log_str)
            # Put some more data in table.
            c = self.session.open_cursor(uri, None)
            if self.logalter:
                myvalue = self.value
            else:
                myvalue = self.value2
            for k in range(self.entries):
                c[k + self.entries] = myvalue
            c.close()
            if self.logalter:
                expected_keys += self.entries
            self.verify_logrecs(expected_keys)
        else:
            msg = '/Cannot alter open table/'
            if special:
                alteruri = suburi
            else:
                alteruri = uri
            self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
                lambda:self.session.alter(alteruri, alter_param), msg)

if __name__ == '__main__':
    wttest.run()
