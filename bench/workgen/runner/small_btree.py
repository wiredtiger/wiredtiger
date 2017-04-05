from wiredtiger import *
from workgen import *

conn = wiredtiger_open("WT_TEST", "create,cache_size=500MB")
s = conn.open_session()
tname = "file:test.wt"
s.create(tname, 'key_format=S,value_format=S')

ops = [Operation(Operation.OP_INSERT, Table(tname), Key(Key.KEYGEN_APPEND, 10), Value(100))]
thread = Thread(OpList(ops * 500000))
pop_workload = Workload(ThreadList([thread]))
execute(conn, pop_workload)
print('populate finished')

op = Operation(Operation.OP_SEARCH, Table(tname), Key(Key.KEYGEN_UNIFORM, 10))
thread = Thread(OpList([op]))
workload = Workload(ThreadList([thread]))
workload._run_time = 120
workload._report_interval = 5
execute(conn, workload)
