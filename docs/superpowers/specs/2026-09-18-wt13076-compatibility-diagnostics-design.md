# WT-13076 Compatibility Diagnostics Design

## Goal

Strengthen the WT-13076 cross-version compatibility workload so it exercises a deeper B-tree with alternating deletes, and make page-stop durable timestamp generation visible in test logs.

## Scope

Only `test/compatibility/suite/test_wt13076.py` changes. The existing upgrade and downgrade scenarios remain intact, and the test continues to verify data with both the develop and MongoDB 9.0 binaries.

## Workload

- Increase the row count from 100 to 10,000.
- Use 512-byte allocation, leaf, and internal page limits to create multiple internal pages.
- For the partial scenario, delete alternating keys (`0, 2, 4, ...`) so no complete leaf page is removed.
- For the full scenario, retain deletion of every key so complete page deletion remains covered.
- Update later mutation and expected-deletion calculations to use the same alternating pattern for partial scenarios.

## Diagnostics

Open each test connection with `verbose=(reconcile)` and emit a concise test-context message identifying the branch, scenario, row count, and deletion mode. WiredTiger's reconcile diagnostics will then show whether page-stop durable timestamps were written or cleared.

The diagnostics are intentionally non-asserting. The test must not depend on the count or exact wording of verbose messages, since reconciliation and log formatting can vary across supported branches. Existing data verification and `wt verify` calls remain the correctness checks.

## Compatibility

The compatibility runner builds both branches with the same non-standalone configuration. The test must not introduce configuration differences between the develop and 9.0 processes.

## Validation

Run the focused compatibility test using the compatibility test harness, then inspect its captured output for:

- no page-stop address records in the alternating partial scenario;
- page-stop address records in the full-delete scenario;
- successful cross-version verification in both upgrade and downgrade directions.
