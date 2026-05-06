# test_prefetch01 — Prefetch availability and enable/disable configuration validation

**File:** `test/suite/test_prefetch01.py`
**Storage mode:** General
**Components under test:** prefetch configuration, connection API, session API

## Test Cases

### `test_prefetch01.test_prefetch_config`
- **What it tests:** Verifies that prefetch cannot be enabled at the connection or session level when `prefetch=(available=false)` is set; covers all combinations of `conn.available`, `conn.default`, and per-session `enabled` flags
- **Components:** `conn/conn_prefetch.c`, `session/session_api.c`
- **Notes:** Scenarios: `available` (true/false) × `default` (true/false) × session config (no-config / enabled=true / enabled=false); expects `WiredTigerError` with message `/pre-fetching cannot be enabled/` when available=false but default=true (at connection open) or enabled=true (at session open); copies DB home to a new directory to open a second connection
