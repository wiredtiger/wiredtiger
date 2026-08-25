# Deterministic checkpoint-cleanup page-removal coverage

## Goal

Add reliable regression coverage proving checkpoint cleanup does not read pages that
were fully removed, while still exercising partial removal across many leaf pages.

## Design

Add a dedicated Python test rather than extending `test_cc09.py`. The existing
`test_cc09.py` delete-loop changes will be reverted because they do not deterministically
control which leaf pages contain removable keys.

The new test will create a table with deliberately small leaf pages and populate a
fixed number of integer keys. It will define independent partial-removal and
full-removal scenarios:

- Partial removal deletes fixed contiguous 10-key ranges separated by fixed 10-key
  ranges that remain present.
- Full removal deletes fixed contiguous 10-key ranges distributed across the table
  until every key is removed.

Each scenario will checkpoint the initial data, reopen the connection so the table is
on disk, perform its removals, checkpoint again, reopen once more, and open the table
to make it eligible for checkpoint cleanup. The test will wait for cleanup using the
existing checkpoint-cleanup synchronization helper and inspect the generic page-read
statistic. Assertions will verify the expected removal state and that cleanup does not
read pages solely because their contents were already fully removed.

The scenarios will use deterministic key ranges and fixed page sizing rather than
timing or incidental page layouts. No new statistics or production code are required.
