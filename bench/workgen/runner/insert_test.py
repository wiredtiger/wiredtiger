from wiredtiger import *
from workgen import *

def tablename(id):
    return "table:test%06d" % id

conn = wiredtiger_open("WT_TEST", "create,cache_size=1G")
s = conn.open_session()
s.create(tablename(0))

ops = [Operation(Operation.OP_INSERT, Table(0), Key(Key.KEYGEN_APPEND, 10), Value(100))]
thread1 = Thread(OpList(ops))
workload = Workload(ThreadList([thread1]))

execute(workload)
