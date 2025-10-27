# Conn

## Compaction

### Overview

Compaction is implemented in [conn_compact.c](conn_compact.c).

### When will a compaction action be skipped

The code implementation is [__background_compact_should_skip](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_compact.c#L233-L311).

The key skip judges are (order matters):
1. A table can be configured to skip compaction using `exclude=["table:a.wt"]`, the configuration is parsed in [__background_compact_exclude_list_process](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_compact.c#L100-L120) , you can find an example in [test_compact06](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/test/suite/test_compact06.py#L38)
1. File size < `1MB`
1. `background_compact.max_file_skip_time` (unit of Seconds), If the elapsed time since the last compaction run for the same URI does not exceed this threshold, the compaction will be skipped.
   1. The configuration of `debug_mode.background_compact` controls the value, we have [hardcoded configurations](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_api.c#L2158-L2176).
1. A previous compaction failure matters. 
   1. Assume the last compaction failed — retrying too soon is likely to fail again, so the retry is skipped to prevent repeated failures.
   1. Combined with the previous condition, this means that within each `max_file_skip_time` period, there can be at most one failed compaction.

### When to remove a compaction stat from the connection list

The cleanup logic is implemented in [__background_compact_list_cleanup](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_compact.c#L412-L447).

The `cleanup_type` matters for action.
- For `EXIT` and `OFF` type, the function will clear all the stats directly.
- For `STALE_STAT` type, the function attempts to remove statistics that have been idle for a long time. 
   - The entrance is from the [__background_compact_server](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_compact.c#L572-L574)
   - In this mode, `background_compact.max_file_idle_time` (unit of Seconds) will be used to identify which stat has not been updated for too long. And same to `max_file_skip_time`, the reference time point is from last compact run.
      - Same to `max_file_skip_time` this is controlled by configuration of `debug_mode.background_compact` with [hardcoded configurations](https://github.com/wiredtiger/wiredtiger/blob/cec2d68c2a51be610745b4ab481c572aaec08d3c/src/conn/conn_api.c#L2158-L2176). 

