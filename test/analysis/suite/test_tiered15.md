# test_tiered15 — session.create "type" configuration with tiered storage

**File:** `test/suite/test_tiered15.py`
**Storage mode:** Tiered (connection-level), also tests non-tiered tables within a tiered connection
**Components under test:** `session.create` with explicit `type=` configuration on tiered and non-tiered tables, error handling for unsupported type values

## Test Cases

### `test_tiered15.test_create_type_config`
- **What it tests:** Verifies the behaviour of the `type=` configuration parameter in `session.create` when the connection has tiered storage enabled. Two dimensions are tested:
  - **Tiered tables** (default tiered storage): only `type=file` is permitted; all other types (`table`, `tier`, `tiered`, `colgroup`, `index`, `backup`) raise "Operation not supported".
  - **Non-tiered tables within a tiered connection** (created with `tiered_storage=(name=none)`): `type=file` and `type=table` succeed; `type=tier` is also accepted; `type=tiered`, `type=index` raise "Invalid argument"; `type=colgroup` is skipped (expected to crash); `type=backup` raises "Operation not supported".
  - Skips the test entirely if the current scenario is `non_tiered` (no tiered connection configured).
- **Components:** `src/schema/schema_create.c`, tiered schema handling, `session.create` type validation
- **Notes:**
  - Parametrized across all storage sources (including non-tiered scenario) × 7 type values (`file`, `table`, `tier`, `tiered`, `colgroup`, `index`, `backup`).
  - The `is_tiered_table` dimension mentioned in the source is defined but not included in `make_scenarios`; the test body uses a local `is_tiered_table` variable derived from scenario name, not from the scenario dict.
  - `type=colgroup` with a non-tiered table is explicitly skipped as it causes a crash (known limitation).
