# test_tiered14 — Randomized sequences of flush, checkpoint, reopen, insert, and update

**File:** `test/suite/test_tiered14.py`
**Storage mode:** Tiered
**Components under test:** flush_tier, checkpoint, connection reopen/recovery, insert, update — all in arbitrary interleaved order; data correctness after each sequence

## Test Cases

### `test_tiered14.test_tiered`
- **What it tests:** Runs 21 distinct operation sequences against a tiered table using the `playback` helper. Each sequence is a string of single-character opcodes: `a` (add up to 100 random keys), `u` (update up to 100 existing keys), `c` (checkpoint), `r` (reopen connection), `f` (flush_tier), `.` (check all inserted data). After each sequence completes, a final `.` check is always appended. The test uses a fixed random seed (`random.seed(0)`) for reproducibility. It starts with a fixed sequence `"aaaaacaaa.uucrauaf.aauaac.auu.aacrauafa.uruua."` and then generates 10 add/update-heavy sequences (weighted `aaaaauuuuufcr.`) and 10 balanced sequences (`aufcr.`).
- **Components:** `src/tiered/conn_tiered.c`, flush_tier path, checkpoint, session reopen, `TrackedSimpleDataSet` / `TrackedComplexDataSet` for data tracking across reopens
- **Notes:**
  - Parametrized on: storage backend (dir_store, s3_store, gcp_store, azure_store) × key format (`i` integer, `S` string) × multiplier (0 = fixed-length keys; 1 = small; 10 = medium; 100 = large, long_only; 1000 = XL, long_only) × dataset type (simple only; complex is commented out).
  - `num_ops` controls the length of each random sequence (100 for dir_store, 20 for s3_store, 100 for gcp/azure).
  - Each `playback` call uses a unique `testnum`-derived URI to avoid cross-sequence bucket namespace collisions.
  - `complex` dataset scenario is commented out (`long_only=True`).
