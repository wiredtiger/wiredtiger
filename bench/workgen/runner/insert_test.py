from wiredtiger import *
from workgen import *

def tablename(id):
    return "table:test%06d" % id

def show(tname):
    print('')
    print('<><><><> ' + tname + ' <><><><>')
    c = s.open_cursor(tname, None)
    for k,v in c:
        print('key: ' + k)
        print('value: ' + v)
    print('<><><><><><><><><><><><>')
    c.close()

context = Context()
conn = wiredtiger_open("WT_TEST", "create,cache_size=1G")
s = conn.open_session()
tname0 = tablename(0)
tname1 = tablename(1)
s.create(tname0, 'key_format=S,value_format=S')
s.create(tname1, 'key_format=S,value_format=S')

ops = [Operation(Operation.OP_INSERT, Table(tname0), Key(Key.KEYGEN_APPEND, 10), Value(100))]
thread0 = Thread(OpList(ops))
workload = Workload(context, ThreadList([thread0]))

execute(conn, workload)
show(tname0)

# The context has memory of how many keys are in all the tables.
# truncate goes behind context's back, but it doesn't matter for
# an insert-only test.
s.truncate(tname0, None, None)

# Show how to 'multiply' operations
op = Operation(Operation.OP_INSERT, Table(tname0), Key(Key.KEYGEN_APPEND, 10), Value(100))
op2 = Operation(Operation.OP_INSERT, Table(tname1), Key(Key.KEYGEN_APPEND, 20), Value(30))
o = op2 * 10
print 'op is: ' + str(op)
print 'multiplying op is: ' + str(o)
thread0 = Thread(OpList([o, op, op]))
workload = Workload(context, ThreadList([thread0]))
execute(conn, workload)
show(tname0)
show(tname1)

s.truncate(tname0, None, None)
s.truncate(tname1, None, None)

# operations can be multiplied, added in any combination.
op += Operation(Operation.OP_INSERT, Table(tname0), Key(Key.KEYGEN_APPEND, 10), Value(10))
op *= 2
op += Operation(Operation.OP_INSERT, Table(tname0), Key(Key.KEYGEN_APPEND, 10), Value(10))
thread0 = Thread(OpList([op * 10 + op2 * 20]))
workload = Workload(context, ThreadList([thread0]))
execute(conn, workload)
show(tname0)
show(tname1)

print('workload is ' + str(workload))
print('thread0 is ' + str(thread0))

opx = None
got_exception = False
try:
    opx = Operation(Operation.OP_INSERT, Table('foo'), Key(Key.KEYGEN_APPEND, 10))
except BaseException as e:
    print('got expected exception: ' + str(e))
    got_exception = True
if not got_exception or opx != None:
    print('*** ERROR: did not get exception')

def expectException(expr):
    gotit = False
    try:
        expr()
    except BaseException as e:
        print('got expected exception: ' + str(e))
        gotit = True
    if not gotit:
        raise Exception("missing expected exception")

def assignit(k, n):
    k._size = n

expectException(lambda: Key(Key.KEYGEN_APPEND, 1))
k = Key(Key.KEYGEN_APPEND, 5)
assignit(k, 30)
assignit(k, 1)  # we don't catch this exception here, but in execute.
op = Operation(Operation.OP_INSERT, Table(tname0), k, Value(10))
workload = Workload(context, ThreadList([Thread(OpList([op]))]))
expectException(lambda: execute(conn, workload))
