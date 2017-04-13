from wiredtiger import *
from workgen import *

conn = wiredtiger_open("WT_TEST", "create,cache_size=500MB")
s = conn.open_session()
tname = "file:test.wt"
s.create(tname, 'key_format=S,value_format=S')
table = Table(tname)

context = Context()
op = Operation(Operation.OP_INSERT, table, Key(Key.KEYGEN_APPEND, 20), Value(100))
thread = Thread(OpList([op * 500000]))
pop_workload = Workload(context, ThreadList([thread]))
print('populate:')
pop_workload.run(conn)

op = Operation(Operation.OP_SEARCH, table, Key(Key.KEYGEN_UNIFORM, 20))
t = Thread(OpList([op]))
workload = Workload(context, ThreadList([t] * 8))
workload.options.run_time = 120
workload.options.report_interval = 5
print('read workload:')
workload.run(conn)
