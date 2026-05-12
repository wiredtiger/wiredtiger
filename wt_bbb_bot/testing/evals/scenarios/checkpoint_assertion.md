Triage this WiredTiger build failure. No Jira or Evergreen access is needed — all evidence is below.

## Ticket
- **Key:** BF-SCENARIO-1
- **Summary:** test_checkpoint_snapshot failed on rhel8 enterprise
- **Status:** Open
- **Priority:** P2
- **Assignee:** Unassigned
- **Variant:** enterprise-rhel-80-64-bit
- **Task:** test_checkpoint

## Evergreen task log (excerpt)

```
[js_test:test_checkpoint_snapshot] 2024-03-14T02:31:17.441+0000 Starting test: jstests/core/txns/test_checkpoint_snapshot.js
[js_test:test_checkpoint_snapshot] 2024-03-14T02:31:42.887+0000 Assertion failure ts_order->ts_seconds != 0 src/third_party/wiredtiger/src/txn/txn_timestamp.c:118
[js_test:test_checkpoint_snapshot] 2024-03-14T02:31:42.891+0000 Backtrace:
    #0  0x00007f3e4b2a1c3b in wiredtiger_abort ()
    #1  0x00007f3e4b2a2f11 in __wt_assert_timestamp_order ()
    #2  0x00007f3e4b3c1a20 in __wt_txn_commit ()
    #3  0x00007f3e4b3c4b88 in __session_commit_transaction ()
    #4  0x00007f3e4b1d0234 in __wt_checkpoint ()
    #5  0x00007f3e4b1d1a4c in __checkpoint_worker ()
    #6  0x00007f3e4b0cc110 in __wt_thread_run ()
```

```
Assertion: ts_order->ts_seconds != 0 at txn_timestamp.c:118
Signal: SIGABRT
```

## Build Baron
- Failure group: 8 variants affected over last 7 days
- Failure rate: 12/47 runs (25.5%)
- First seen: 2024-03-10
- Last seen: 2024-03-14
- Blocking: mongodb-mongo-master trunk
