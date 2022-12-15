Here are some basic steps for running a workload, generating a trace and analysing it.

## Generate the data (can be reused for read-only workloads to save time)
```sh
suffix= ; ../../build/bench/wtperf/wtperf -O ../bench/wtperf/runners/medium-btree$suffix.wtperf -o create=true -o run_ops=0 -o run_time=0
```

## Run the workload and generate a trace
```sh
suffix= ; ../../build/bench/wtperf/wtperf -O ../bench/wtperf/runners/medium-btree$suffix.wtperf -C 'verbose=[cache_sampling]' -o create=false > medium-btree$suffix.trace 2>&1
```

## Process the trace
```sh
suffix= ; python3 process-trace.py < medium-btree$suffix.trace > medium-btree$suffix.csv
```

Change `suffix` above to run different workloads, e.g., `suffix=-zipfian ...` to repeat these steps with skewed access to keys.