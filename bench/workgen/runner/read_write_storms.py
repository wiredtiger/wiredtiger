#/usr/bin/env python
# generated from runner/read_write_heavy.wtperf originally, then hand edited.

from runner import *
from wiredtiger import *
from workgen import *

context = Context()
conn_config = ""
conn_config += ",cache_size=2GB,eviction=(threads_max=8),log=(enabled=true),session_max=250,statistics=(fast),statistics_log=(wait=1,json),io_capacity=(total=30M)"   # explicitly added
conn = wiredtiger_open("WT_TEST", "create," + conn_config)
s = conn.open_session("")

wtperf_table_config = "key_format=S,value_format=S,type=lsm," +\
    "exclusive=true,allocation_size=4kb," +\
    "internal_page_max=64kb,leaf_page_max=4kb,split_pct=100,"
compress_table_config = "block_compressor=snappy,"
table_config = "memory_page_max=10m,leaf_value_max=64MB,checksum=on,split_pct=90,type=file,log=(enabled=false),leaf_page_max=32k,block_compressor=snappy"
tables = []
table_count = 100
for i in range(0, table_count):
    tname = "table:test" + str(i)
    table = Table(tname)
    s.create(tname, wtperf_table_config +\
             compress_table_config + table_config)
    table.options.key_size = 20
    table.options.value_size = 7000
    tables.append(table)

populate_threads = 4
icount = 4000000
# There are multiple tables to be filled during populate,
# the icount is split between them all.
pop_ops = Operation(Operation.OP_INSERT, tables[0])
pop_ops = op_multi_table(pop_ops, tables)
nops_per_thread = icount / (populate_threads * table_count)
pop_thread = Thread(pop_ops * nops_per_thread)
pop_workload = Workload(context, populate_threads * pop_thread)
pop_workload.run(conn)
print('populate complete')

# Log like file, requires that logging be enabled in the connection config.
log_name = "table:log"
s.create(log_name, wtperf_table_config + "key_format=S,value_format=S," + compress_table_config + table_config + ",log=(enabled=true)")
log_table = Table(log_name)

ops = Operation(Operation.OP_UPDATE, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
thread0 = Thread(ops)
# These operations include log_like operations, which will increase the number
# of insert/update operations by a factor of 2.0. This may cause the
# actual operations performed to be above the throttle.
thread0.options.throttle=11
thread0.options.throttle_burst=0

ops = Operation(Operation.OP_SEARCH, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
thread1 = Thread(ops)
thread1.options.throttle=60
thread1.options.throttle_burst=0

ops = Operation(Operation.OP_SLEEP, "60") + \
      Operation(Operation.OP_CHECKPOINT, "")
checkpoint_thread = Thread(ops)

ops = Operation(Operation.OP_SLEEP, "0.1") + \
      Operation(Operation.OP_LOG_FLUSH, "")
logging_thread = Thread(ops)

############################################################################
# This part was added to the generated file.
# Add threads that do a bunch of operations and sleep, all in a loop.
# At the beginning of the run the threads will tend to be synchronized,
# but that effect will dissipate over time.

ops = Operation(Operation.OP_UPDATE, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
ops = ops * 10000 + Operation(Operation.OP_SLEEP, "10")
thread_big_10 = Thread(ops)

ops = Operation(Operation.OP_UPDATE, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
ops = ops * 80000 + Operation(Operation.OP_SLEEP, "20")
thread_big_20 = Thread(ops)

ops = Operation(Operation.OP_SEARCH, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
ops = ops * 10000 + Operation(Operation.OP_SLEEP, "8")
thread_bigread_8 = Thread(ops)

ops = Operation(Operation.OP_SEARCH, tables[0])
ops = op_multi_table(ops, tables, False)
ops = op_log_like(ops, log_table, 0)
ops = ops * 80000 + Operation(Operation.OP_SLEEP, "16")
thread_bigread_16 = Thread(ops)

# End of added section.
# The new threads will also be added to the workload below.
############################################################################

workload = Workload(context, 80 * thread0 + 80 * thread1 + checkpoint_thread + logging_thread + 10 * thread_big_10 + 10 * thread_big_20 + 10 * thread_bigread_8 + 10 * thread_bigread_16)
workload.options.report_interval=1
workload.options.run_time=900
workload.options.sample_rate=1
workload.options.warmup=0
workload.options.sample_interval_ms = 1000
workload.run(conn)

latency_filename = "WT_TEST/latency.out"
latency.workload_latency(workload, latency_filename)
