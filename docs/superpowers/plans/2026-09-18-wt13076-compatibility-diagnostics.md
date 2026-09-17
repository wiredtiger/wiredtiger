# WT-13076 Compatibility Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the WT-13076 compatibility workload to exercise deeper internal trees with alternating deletes and expose page-stop durable timestamp reconciliation in compatibility logs.

**Architecture:** Keep the existing upgrade and downgrade scenario flow unchanged. Modify the single compatibility test so both branch processes use the same larger workload, page sizing, delete-pattern helpers, and reconcile verbosity; correctness remains covered by existing cross-version reads and `wt verify`, while verbose output is diagnostic only.

**Tech Stack:** Python WiredTiger compatibility suite, WiredTiger Python API, `wt verify`, CMake-built develop and MongoDB 9.0 binaries.

---

### Task 1: Expand the compatibility workload and diagnostics

**Files:**
- Modify: `test/compatibility/suite/test_wt13076.py:37-150`

- [ ] **Step 1: Replace the small workload constants and add scenario helpers**

Set `nrows` to `10000`, retain 512-byte allocation, leaf, and internal limits, and add helpers that make the delete pattern explicit:

```python
    nrows = 10000
    table_config = 'key_format=i,value_format=S,allocation_size=512,leaf_page_max=512,internal_page_max=512'

    def _deleted_keys(self, start, count):
        if self.delete_all:
            return range(start, start + count)
        return range(start, start + count, 2)

    def _log_scenario(self, branch_name, operation):
        pattern = 'all keys' if self.delete_all else 'alternating keys'
        self.prhead(
            'WT-13076 {} on {}: rows={}, delete_pattern={}, page_config={}'.format(
                operation, branch_name, self.nrows, pattern, self.table_config))
```

- [ ] **Step 2: Use the helpers for initial creation and later mutations**

Open test connections with reconcile verbosity and replace contiguous partial-delete ranges with `_deleted_keys`:

```python
        self._log_scenario('current branch', 'creating and deleting')
        conn = wiredtiger.wiredtiger_open('.', 'create,statistics=(all),verbose=(reconcile)')
```

Use:

```python
        deleted = self._deleted_keys(0, self.nrows)
```

in `_create_deleted_pages`, and:

```python
        deleted = set(self._deleted_keys(0, self.nrows))
        for round_number in range(rounds):
            start = self.nrows + round_number * 20
            deleted.update(self._deleted_keys(start, 20))
```

in `_verify_pages`, adjusted so the set contains every deleted range already processed. In `_mutate_pages`, use:

```python
        deleted = self._deleted_keys(start, 20)
```

and open the connection with:

```python
        conn = wiredtiger.wiredtiger_open('.', 'statistics=(all),verbose=(reconcile)')
```

The reconcile diagnostics emitted to the captured stderr stream must remain informational; do not add assertions against message counts or exact log text.

- [ ] **Step 3: Run Python syntax and style checks**

Run:

```bash
python3 -m py_compile test/compatibility/suite/test_wt13076.py
cd dist && ./s_fast
```

Expected: Python compilation succeeds and `s_fast` exits with status 0. Any existing comment warning about committed WT-13076 documentation is non-fatal.

- [ ] **Step 4: Review the diff for scenario correctness**

Run:

```bash
git diff --check
git diff -- test/compatibility/suite/test_wt13076.py
```

Expected: Only the intended compatibility test changes are present; the upgrade/downgrade method ordering is unchanged, and the partial scenario uses alternating deletes while the full scenario deletes all keys.

- [ ] **Step 5: Commit the test changes**

```bash
git add test/compatibility/suite/test_wt13076.py
git commit -m "WT-13076 expand compatibility aggregate coverage" -m \
  "Exercise deeper trees with alternating deletes and enable reconcile diagnostics for page-stop aggregate behavior." -m \
  "Co-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"
```

### Task 2: Run the matching-format compatibility scenarios

**Files:**
- Test: `test/compatibility/suite/test_wt13076.py`
- Validation output: compatibility harness captured `stdout.txt` and `stderr.txt`

- [ ] **Step 1: Build both compatibility branches with the same configuration**

Run the compatibility harness setup with `WT_STANDALONE_BUILD=0` for both branches:

```bash
cd test/compatibility/suite
python3 compatibility_test.py -v 2
```

Expected: The develop and MongoDB 9.0 branch builds use the same non-standalone B-tree format. If local SWIG headers are unavailable, use the already-built matching-format binaries and record the build limitation rather than mixing a standalone binary with the 9.0 binary.

- [ ] **Step 2: Run both WT-13076 directions**

Run the focused compatibility suite through the harness and inspect the four WT-13076 scenario directories:

```bash
find WT_TEST -path '*test_wt13076*' -name stderr.txt -print
grep -R "writing address with page stop durable" WT_TEST/test_wt13076*
grep -R "clearing page stop durable" WT_TEST/test_wt13076*
```

Expected:

- Alternating partial-delete scenarios show no page-stop address records.
- Full-delete scenarios show page-stop address records.
- Both upgrade and downgrade scenarios complete successfully.
- The matching 9.0 `wt verify` operations pass.

- [ ] **Step 3: Confirm the repository remains clean after validation**

Run:

```bash
git status --short --branch
```

Expected: The only tracked change is the committed compatibility-test update. Remove any generated compatibility build or test artifacts outside the repository before reporting completion.
