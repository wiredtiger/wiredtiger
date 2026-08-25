# Simplified checkpoint-cleanup page-removal coverage

## Goal

Make the removal patterns in `test_cc12.py` clear without changing its
checkpoint-cleanup assertion.

## Design

The partial-removal scenario will continue to delete the first 10 keys of each
20-key group. The remaining 10 keys in every group stay present.

The full-removal scenario will delete every key from zero through the final key
in one pass. It will no longer rely on the two complementary alternating-range
passes used by the partial-removal scenario.

Both scenarios retain the existing initial checkpoint, removal checkpoint,
reopen, checkpoint-cleanup trigger, and assertion that cleanup does not read
the data source.
