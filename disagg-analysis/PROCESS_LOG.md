# Process Log: DisAgg Disabled Tests Analysis

**Date:** 2026-05-06  
**Task:** Collect all tests disabled for DisAgg, research why, compile into analysis folder.

---

## Steps Performed

### 1. Discovered repository structure
```bash
find . -name "hook_disagg.py" -o -name "*disagg*" 2>/dev/null
```
Found: `test/suite/hook_disagg.py` is the main hook file.

### 2. Identified the three skip mechanisms

**Mechanism 1 — `@wttest.skip_for_hook` decorator:**
```bash
grep -r "skip_for_hook.*disagg" test/suite --include="*.py" -n | sort
```
Returns ~65 usages across 36 files.

**Mechanism 2 — Category-based skips in `hook_disagg.py`:**
Read `hook_disagg.py` lines 369–392 — the `should_skip()` method.

**Mechanism 3 — `hook_disagg.fail` file:**
```bash
find test/suite -name "hook_disagg.fail"
cat test/suite/hook_disagg.fail
```
55 test files listed.

### 3. Collected FIXME ticket numbers

From the codebase:
```bash
grep -r "FIXME.*disagg\|FIXME-WT" test/suite/hook_disagg.py
grep -rn "FIXME-WT" test/suite --include="*.py" | grep -i disagg
```

Ticket numbers found: WT-17177, WT-16532, WT-14740, WT-14563, WT-14582, WT-16757, WT-16920, WT-16918, WT-14937, WT-15064

From `hook_disagg.fail`:
Ticket numbers: WT-15507, WT-15369, WT-15189, WT-15370, WT-15371, WT-15372, WT-16182, WT-16872, WT-15474

### 4. Queried Jira tickets via MCP

Used `mcp__devprod-mcp-gateway__jira_get_issue` for each ticket (parallel batches).

Key findings:
- **WT-15370** (test_durable_ts01) — CLOSED/Fixed May 6, 2026
- **WT-15371** (test_hs01) — CLOSED/Fixed May 6, 2026
- **WT-15507** (test_checkpoint06) — CLOSED/Fixed Apr 24, 2026
- **WT-16182** (test_timestamp26) — CLOSED/Fixed Apr 24, 2026
- **WT-15474** (test_truncate01) — CLOSED/Won't Fix Apr 30, 2026
- **WT-14937** — CLOSED (referenced but not in fail file anymore)
- **WT-16872** (4 tests in fail file) — In Code Review

### 5. Checked git history for context
```bash
git log --oneline --all --grep="skip.*disagg\|disagg.*skip\|WT-14740|WT-16532..." --since="2024-01-01"
```

Notable commits:
- `ae88f255` — "Skip test that cannot run on disagg" (Apr 14, 2026) — truncate19
- `3426ed39` — "Fix disagg guards with accurate root cause comments" (Apr 22, 2026) — test_stat10
- `0b98a0e2` — "WT-17143 Explicitly disable read-only connections with disagg" (merged)

### 6. Identified stale `hook_disagg.fail` entries

5 entries in `hook_disagg.fail` have corresponding closed tickets:
- `test_checkpoint06.py` (WT-15507, Fixed)
- `test_durable_ts01.py` (WT-15370, Fixed)
- `test_hs01.py` (WT-15371, Fixed)
- `test_timestamp26.py` (WT-16182, Fixed)
- `test_truncate01.py` (WT-15474, Won't Fix / re-enabled via WT-17328)

### 7. Produced output file

`disagg-analysis/disabled_tests_analysis.md`

---

## Key Files

| File | Purpose |
|------|---------|
| `test/suite/hook_disagg.py` | Main disagg hook — all skip logic lives here |
| `test/suite/hook_disagg.fail` | Evergreen-level exclusion list |
| `test/suite/wttest.py:1111` | `skip_for_hook` decorator definition |
| `test/suite/wthooks.py` | Hook infrastructure (HOOK_REPLACE, register_skipped_test) |
| `test/suite/helper_disagg.py` | DisaggConfigMixin, gen_disagg_storages |

## MCP Tools Used

- `mcp__devprod-mcp-gateway__jira_get_issue` — fetch individual Jira tickets by key

## Notes for Future Runs

- `hook_disagg.fail` contains stale entries — several closed tickets are still listed. Running the analysis again, check if these have been cleaned up.
- WT-16872 (is_layered threading fix) was In Code Review as of 2026-05-06. When it merges, `test_prepare28.py`, `test_error_info03.py`, `test_hs24.py`, `test_hs_evict_race01.py` should be removable from the fail list.
- WT-15369 (cursor13/cursor21 stats) had a PR reverted — currently back in Open status with active work.
- WT-15189 (clayered_next_random timeout) has an open PR but is a CI-blocker.
