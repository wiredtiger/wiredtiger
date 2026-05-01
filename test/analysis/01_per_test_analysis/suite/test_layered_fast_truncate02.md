# test_layered_fast_truncate02 — Follower visibility and cursor behavior with fast-truncated checkpoints

**File:** `test/suite/test_layered_fast_truncate02.py`
**Storage mode:** Disagg/Layered (disagg_only). Skipped if `wiredtiger.disagg_fast_truncate_build() == 0`.
**Components under test:** Follower pickup of fast-truncated stable pages, MVCC visibility at and before truncation timestamp, forward/backward cursor scan skipping truncated range, `search_near` on deleted key, pre-truncation reads on a follower

## Setup (shared across all tests)
Leader writes 5000 rows (integer keys 1–5000, value `"a" * 500`) at ts=10 and checkpoints. All pages are evicted (using `debug=(release_evict)`) to force the leader to use page-level fast-delete markers. The truncation range is rows 1001–4000. The follower opens by calling `disagg_advance_checkpoint()`. Tests parametrized by disagg storage variant.

### `test_layered_fast_truncate02.test_visibility`
- **What it tests:** Leader fast-truncates [1001, 4000] at ts=20 and checkpoints. Follower advances and verifies: at `read_timestamp=20`, keys 1001, 2500 (midpoint), and 4000 return `WT_NOTFOUND`; keys 1, 1000, 4001, and 5000 return 0 with the original value. At `read_timestamp=15` (before truncation), keys 1001, 2500, and 4000 all return 0 with the original value. Verifies basic MVCC truncation visibility: truncation visible at or after ts=20, invisible before.
- **Components:** Fast-delete page visibility on follower, MVCC time-window on truncated pages

### `test_layered_fast_truncate02.test_pre_truncation_read_sees_all_rows`
- **What it tests:** Same setup (truncate at ts=20). Follower reads at `read_timestamp=10` (the original insert timestamp, before truncation): keys 1001, 2500, and 4000 must all return 0 with original values. Then performs a full forward scan at `read_timestamp=10` and asserts that exactly 5000 rows are returned (all rows visible at the pre-truncation timestamp). Verifies MVCC correctness: pre-truncation reads on a follower must not lose any data.
- **Components:** Full scan at pre-truncation timestamp on follower; MVCC across checkpoint boundary

### `test_layered_fast_truncate02.test_cursor_scanning`
- **What it tests:** Same setup (truncate at ts=20). Follower performs:
  - **Forward scan** at `read_timestamp=25`: iterates all rows, asserts no key in [1001, 4000] is visited, counts total rows (must equal 5000 − 3000 = 2000), and asserts the first key after the gap is 4001 (the one immediately after the truncated range).
  - **Backward scan** at `read_timestamp=25`: same count and gap check; asserts the first key before the gap when scanning backward is 1000.
  - **search_near on midpoint (2500)** at `read_timestamp=25`: must land outside the truncated range; `cmp` is −1 (landed below) or +1 (landed above); landed key must be 1000 (if cmp=−1) or 4001 (if cmp=+1) — never inside [1001, 4000].
- **Components:** Cursor skip of fast-deleted pages during forward/backward scan, `search_near` landing on boundary near truncation
