from wiredtiger import *
from workgen import *

conn = wiredtiger_open("WT_TEST", "create,cache_size=500MB")
s = conn.open_session()
tname = "table:test"
s.create(tname, 'key_format=S,value_format=S')
table = Table(tname)

context = Context()
op = Operation(Operation.OP_INSERT, table, Key(Key.KEYGEN_APPEND, 20), Value(100))
thread = Thread(OpList([op * 500000]))
pop_workload = Workload(context, ThreadList([thread]))
print('populate:')
pop_workload.run(conn)

opread = Operation(Operation.OP_SEARCH, table, Key(Key.KEYGEN_UNIFORM, 20))
opwrite = Operation(Operation.OP_INSERT, table, Key(Key.KEYGEN_APPEND, 20), Value(100))
treader = Thread(OpList([opread]))
twriter = Thread(OpList([txn(opwrite * 2)]))
workload = Workload(context, ThreadList([treader] * 8 + [twriter] * 2))
workload.options.run_time = 10
workload.options.report_interval = 5
print('transactional write workload:')
workload.run(conn)
