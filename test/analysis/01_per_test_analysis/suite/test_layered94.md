# test_layered94 — Prepared transactions survive follower step-up and can be resolved

**File:** `test/suite/test_layered94.py`
**Storage mode:** Disagg/Layered
**Components under test:** Prepared transaction lifecycle across step-up, `preserve_prepared=true`, prepared INSERT/UPDATE/DELETE, multi-table prepared transactions

## Test Cases

### `test_layered94.test_prepare_insert_survives_step_up`
- **What it tests:** Four-phase test for prepared INSERTs (keys 4–6): Phase 1 — leader commits base data (keys 1–3, ts=60), then either (a) prepares keys 4–6 at ts=100 and checkpoints at stable=150 with the prepare active (`in_checkpoint=True`), or (b) checkpoints before the prepare (`in_checkpoint=False`). Leader closes with `skip_checkpoint=true`. Phase 2 — follower loads the checkpoint and replays the same prepared INSERT (same prepare_timestamp=100 and prepared_id=123). Phase 3 — follower steps up via `reconfigure(role="leader")` while the prepare is live. Phase 4 — resolves the prepare (commit at ts=200 or rollback at ts=210). Verifies: at ts=60, keys 1–3 present, keys 4–6 absent (on all tables if multi_table). At ts=200: if commit, keys 4–6 present with "prepared_value_N"; if rollback, keys 4–6 absent.
- **Components:** `src/conn/conn_layered_ingest.c` (step-up), `src/txn/txn_prepare.c`, `preserve_prepared=true`
- **Notes:** Parametrized by resolve (commit/rollback) × in_checkpoint (True/False) × multi_table (True/False). When multi_table=True, a second table (`test_layered94_b`) is included in the same prepared transaction.

### `test_layered94.test_prepare_update_survives_step_up`
- **What it tests:** Same four-phase structure as `test_prepare_insert_survives_step_up` but for prepared UPDATEs on keys 1–3 (original values committed at ts=60; updated values prepared at ts=100 with prepared_id=456). Verify: at ts=60, original values on all tables. At ts=200: if commit, updated values; if rollback, original values restored.
- **Components:** Prepared UPDATE across step-up, `preserve_prepared=true`
- **Notes:** Same parametrization. The leader rolls back its own prepare after capturing checkpoint_meta (`in_checkpoint=True` path) to avoid conflict at connection close.

### `test_layered94.test_prepare_delete_survives_step_up`
- **What it tests:** Same four-phase structure for prepared DELETEs on keys 1–3 (committed at ts=60; deletion prepared at ts=100 with prepared_id=789). Verify: at ts=60, original values on all tables. At ts=200: if commit, keys 1–3 return `WT_NOTFOUND`; if rollback, original values.
- **Components:** Prepared DELETE across step-up, `preserve_prepared=true`
- **Notes:** Same parametrization. Leader closes with `skip_checkpoint=true` to ensure the follower loads exactly the checkpoint state without a subsequent clean shutdown checkpoint.
