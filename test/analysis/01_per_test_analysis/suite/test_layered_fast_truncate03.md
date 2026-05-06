# test_layered_fast_truncate03 — Follower stable-page cleanliness and state persistence for fast-truncated pages

**File:** `test/suite/test_layered_fast_truncate03.py`
**Storage mode:** Disagg/Layered (disagg_only). Skipped if `wiredtiger.disagg_fast_truncate_build() == 0`.
**Components under test:** `cache_pages_dirty` stat, `cache_read_deleted` stat, follower stable-page immutability, fast-truncated state survival through eviction and connection reopen, ingest writes restoring subset of truncated keys

## Setup (shared across all tests)
Leader writes 5000 integer-keyed rows (value `"a" * 500`) at ts=10, checkpoints, then evicts all pages (`debug=(release_evict)`) to force page-level fast-delete markers. Truncation range is [1001, 4000]. Tests parametrized by disagg storage variant.

### `test_layered_fast_truncate03.test_no_dirty_on_read`
- **What it tests:** Leader fast-truncates [1001, 4000] at ts=20. Follower advances. Records `cache_pages_dirty` stat before touching any page. Reads every 10th key in [1001, 4000] at `read_timestamp=25` — each returns `WT_NOTFOUND`. Verifies `cache_pages_dirty` did not increase (reading a fast-deleted page on the follower must not dirty it). Then evicts the same key range (`debug=(release_evict)` at ts=10). Re-reads the same sample: each still returns `WT_NOTFOUND` and `cache_pages_dirty` still has not grown. Verifies that neither the initial read nor the post-eviction reload of fast-deleted pages dirties stable pages.
- **Components:** Stable-page immutability on follower, fast-delete read path, `cache_pages_dirty`

### `test_layered_fast_truncate03.test_page_split_with_ingest_writes`
- **What it tests:** Same setup but with `leaf_page_max=4096` (small pages, so truncated range spans many leaf pages). Follower reads each 10th key in [1001, 4000] at ts=25 — all `WT_NOTFOUND`; `cache_pages_dirty` unchanged. Evicts that range, then the follower advances the checkpoint (leader emits an additional checkpoint at ts=20). Follower ingest writes a subset of truncated keys (every 3rd sample key) at ts=30 with value `f'ingest_{key}'`. At `read_timestamp=30`: ingest-restored keys return 0 with their new values; all other sample keys (not restored) remain `WT_NOTFOUND`. At `read_timestamp=25` (before the ingest write): even the restored-by-ingest keys must still return `WT_NOTFOUND`. Verifies: (1) small-page truncation does not dirty pages; (2) ingest can selectively restore a subset of truncated keys; (3) MVCC timestamps correctly separate the truncation-visible and ingest-restore-visible eras.
- **Components:** Multi-leaf-page fast truncate on follower, ingest write restoring truncated key, MVCC across stable truncation + ingest restore

### `test_layered_fast_truncate03.test_state_preserved_on_reopen`
- **What it tests:** Leader fast-truncates [1001, 4000] at ts=20 and checkpoints. The `open_follower()` + verify cycle runs twice: each iteration independently opens the follower connection, advances to the checkpoint, and checks that truncated keys (1001, 1101, 4000) return `WT_NOTFOUND` at ts=25 while non-truncated keys (1, 1000, 4001, 5000) return 0. The follower connection is closed between iterations. Verifies that fast-truncated state survives a complete cold-start reopen of the follower — no in-memory state is required to reconstruct the deleted key set.
- **Components:** Persistent fast-truncate state across follower connection reopen, stable checkpoint fidelity

### `test_layered_fast_truncate03.test_instantiation_not_globally_visible`
- **What it tests:** Leader fast-truncates [1001, 4000] at ts=20 and checkpoints. Follower advances. Records `cache_pages_dirty` and `cache_read_deleted` before any read. Reads key 1101 at `read_timestamp=10` (before truncation) — must return 0 with the original value. After this read: asserts `cache_read_deleted` increased (page was loaded from the fast-delete on-disk representation to instantiate the full page for the pre-truncation read), and `cache_pages_dirty` did not increase (instantiation must not dirty the stable page). Evicts key 1101. Verifies that reading a fast-deleted page at a pre-truncation timestamp correctly instantiates the page for MVCC, increments `cache_read_deleted`, but still does not dirty the stable btree.
- **Components:** `cache_read_deleted` stat, page instantiation from fast-delete marker at pre-truncation read_timestamp, stable-page cleanliness after forced instantiation
