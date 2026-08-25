# Checkpoint Cleanup Page Removal Regression Test

## Goal

Add a statistic that reports on-disk pages selected for reading by checkpoint
cleanup, and use it to verify cleanup reads are observable separately from
obsolete time-window reads.

## Approach

Extend `test/suite/test_cc08.py` with an assertion for the new statistic in the
existing logged-table reclaim-space scenario, where checkpoint cleanup is
already guaranteed to select and read pages.

The new statistic counts every on-disk page selected for reading by checkpoint
cleanup, regardless of whether the reason is reclaim space, obsolete content,
or complete page removal. The existing
`checkpoint_cleanup_pages_read_obsolete_tw` and
`checkpoint_cleanup_pages_read_reclaim_space` statistics remain unchanged for
their existing behaviors.

## Synchronization and assertions

Increment the new statistic in the on-disk cleanup page-selection path after
all skip checks have selected the page for reading. Use the existing
checkpoint-cleanup wait helper rather than fixed sleeps. Capture the read
counter before cleanup and compare the delta, so unrelated reads during setup
do not affect the assertion.

## Scope

Add the statistic through `dist/stat_data.py` and regenerate the generated
statistic declarations and implementations with `dist/s_all`. The production
change is limited to statistic accounting in checkpoint cleanup; the test
should run under the existing test registration and scenario infrastructure.
