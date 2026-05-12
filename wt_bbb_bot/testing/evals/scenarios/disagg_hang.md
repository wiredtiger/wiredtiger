Triage this WiredTiger build failure. No Jira or Evergreen access is needed — all evidence is below.

## Ticket
- **Key:** BF-SCENARIO-2
- **Summary:** test_disagg_checkpoint timed out on rhel8 disaggregated storage variant
- **Status:** Open
- **Priority:** P2
- **Assignee:** Unassigned
- **Variant:** enterprise-rhel-80-64-bit-disagg
- **Task:** test_disagg_checkpoint

## Evergreen task log (excerpt)

```
[2024-03-15T04:11:02.001+0000] Starting test_disagg_checkpoint
[2024-03-15T04:11:03.442+0000] Opening WiredTiger connection with disagg storage source
[2024-03-15T04:11:03.891+0000] Starting checkpoint thread
[2024-03-15T04:11:04.102+0000] Checkpoint 1 started
[2024-03-15T04:11:04.210+0000] Flushing dirty pages to page service...
... (no further output) ...
[2024-03-15T04:21:04.001+0000] Timeout: task exceeded 600 seconds with no output
[2024-03-15T04:21:04.003+0000] Sending SIGTERM to process group
```

No crash, no assertion. The process stopped producing output after "Flushing dirty pages to page service..."

## Build Baron
- Failure group: 3 variants affected (all disagg variants)
- Failure rate: 7/10 runs (70%)
- First seen: 2024-03-13
- Last seen: 2024-03-15
- Blocking: mongodb-mongo-master trunk (disagg storage path only)
