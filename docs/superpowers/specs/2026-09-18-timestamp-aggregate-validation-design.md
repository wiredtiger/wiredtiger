# Timestamp Aggregate Parent Validation Design

## Problem

The WT-13076 timestamp aggregate changes retain `WT_TS_MAX` as the sentinel for
an unknown or unbounded newest stop timestamp. Parent validation currently
compares that sentinel as if it were an ordinary timestamp. During
`test_rollback_to_stable34`, a child aggregate with `newest_stop_ts =
WT_TS_MAX` is rejected against a parent with `newest_stop_ts = 35`, even
though the sentinel does not represent a comparable stop timestamp.

## Design

Update aggregate parent validation so the `newest_stop_ts` ordering check is
performed only when the child has an ordinary stop timestamp. Keep the current
ordering check unchanged for all finite, meaningful stop timestamps. Do not
remove or weaken validation of `newest_durable_ts`, page-stop durable
timestamps, oldest-start timestamps, newest transactions, or stop transactions.

The sentinel remains unchanged in memory, metadata, and on-disk encoding. This
is a validation-only correction and does not introduce a new metadata version
or alter timestamp interpretation.

## Testing

Add focused regression coverage for the rollback-to-stable scenario that
produced the failure, ensuring a child aggregate with `WT_TS_MAX` does not fail
parent validation. Preserve coverage for ordinary stop timestamps so a child
timestamp later than its parent still fails validation.

Run the targeted rollback-to-stable test, repeat it to check determinism, run
the relevant C/C++ test target if available, and run `dist/s_fast` or the
repository-required formatting checks.

## Error handling and compatibility

Existing validation errors and `EINVAL` behavior remain unchanged for
comparable timestamps. Existing and older checkpoint metadata continue to use
the same sentinel values and parsing behavior.
