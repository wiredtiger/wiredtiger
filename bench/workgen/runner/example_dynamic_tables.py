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

import threading as pythread
import random
import string
import time

from runner import *
from wiredtiger import *
from workgen import *


def generate_random_string(length):
    assert length > 0
    characters = string.ascii_letters + string.digits
    str = ''.join(random.choice(characters) for _ in range(length))
    return str


def create(session, workload, table_config):

    # Generate a random name.
    name_length = 10
    table_name = "table:" + generate_random_string(name_length)

    try:
        session.create(table_name, table_config)
        # This indicates Workgen a new table exists.
        workload.create_table(table_name)
    # Collision may occur.
    except RuntimeError as e:
        assert "already exists" in str(e).lower()


context = Context()

connection = context.wiredtiger_open("create")
session = connection.open_session()

# Create a table.
table_config = 'key_format=S,value_format=S'
table_name = 'table:simple'
session.create(table_name, table_config)

# Work on the table for some time.
ops = Operation(Operation.OP_INSERT, Table(table_name), Key(Key.KEYGEN_APPEND, 10), Value(40))
thread = Thread(ops)
workload = Workload(context, thread)
workload.options.run_time = 10

# Start the workload.
workload_thread = pythread.Thread(target=workload.run, args=([connection]))
workload_thread.start()

# Create tables while the workload is running.
create_interval_sec = 1

while workload_thread.is_alive():
    create(session, workload, table_config)
    time.sleep(create_interval_sec)

workload_thread.join()
