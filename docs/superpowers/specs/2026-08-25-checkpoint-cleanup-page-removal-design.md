# Checkpoint Cleanup Page Removal Regression Test

## Goal

Add regression coverage proving checkpoint cleanup does not load a page solely
to reclaim it while the page still contains live records, but does load it
after the page is fully removed.

## Approach

Extend `test/suite/test_cc09.py` with a logged-table scenario using the
checkpoint cleanup reclaim-space mode. The test will create data spanning
multiple leaf pages, remove records from one page without removing the entire
page, and run checkpoint cleanup. It will assert that
`checkpoint_cleanup_pages_read_reclaim_space` does not increase. The test will
then remove the remaining records from that page, run cleanup again, and assert
that the same statistic increases.

The existing statistic is used because it specifically counts pages loaded for
reclaim-space processing. The obsolete-time-window read statistic is not
appropriate for this behavior.

## Synchronization and assertions

Use the existing checkpoint-cleanup wait helper rather than fixed sleeps.
Capture the reclaim-space read counter before each cleanup run and compare
counter deltas, so unrelated reads that occurred during setup do not affect
the assertions. Keep the test deterministic by using small leaf pages and
known key ranges, and use a logged table because reclaim-space page reads are
restricted to logged tables.

## Scope

No production code or new statistic is required. The test is limited to the
checkpoint cleanup Python suite and should run under the existing test
registration and scenario infrastructure.
