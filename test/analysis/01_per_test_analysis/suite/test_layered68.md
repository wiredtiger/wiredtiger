# test_layered68 — Address cookie upgrade/downgrade compatibility

**File:** `test/suite/test_layered68.py`
**Storage mode:** Disagg/Layered
**Components under test:** Disagg address cookie versioning, `disagg_address_cookie_upgrade`, `disagg_address_cookie_optional_field`, checkpoint pickup, node restart

## Test Cases

### `test_layered68.test_layered68`
- **What it tests:** Three-phase test of address cookie version compatibility across node restarts. Phase 1: starts as leader, writes 2000 keys, checkpoints, then restarts without local files using the newer cookie format (`disagg_address_cookie_upgrade` + `disagg_address_cookie_optional_field` from the scenario), verifies data, steps up as leader, modifies 100 keys, checkpoints. Phase 2: restarts with the older cookie format (`disagg_address_cookie_upgrade=none`), attempts to pick up the latest checkpoint: if `compatible=True` (upgrade=none or compatible), the reconfigure succeeds and all 2000 keys are readable; if `compatible=False` (upgrade=incompatible), `reconfigure(checkpoint_meta=...)` raises `WiredTigerError` with "Unsupported disaggregated address cookie version". For the compatible case, steps up and modifies keys 100–199, checkpoints again. Phase 3: restarts with the newer cookie format again, verifies all 2000 keys match expected values.
- **Components:** `src/block_disagg/block_disagg.c`, address cookie serialization/deserialization, `src/conn/conn_disagg.c`
- **Notes:** Parametrized by `address_cookie_upgrade` (none/compatible/incompatible) × `optional_field` (false/true). Tests backward compatibility of the address cookie wire format between newer writer and older reader. `table:` URI with `type=layered`. Uses `restart_without_local_files()` from `DisaggConfigMixin`.
