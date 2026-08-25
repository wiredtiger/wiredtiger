# Simplified checkpoint-cleanup page-removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `test_cc12.py` explicitly delete all keys in its full-removal scenario.

**Architecture:** Keep the partial scenario's alternating 10-key removals. Replace the full scenario's complementary alternating pass with a single transaction that removes every key, preserving the checkpoints, reopen, and no-page-read assertion.

**Tech Stack:** WiredTiger Python test suite, `wttest`, `wtscenario`.

---

### Task 1: Simplify full removal

**Files:**
- Modify: `test/suite/test_cc12.py:43-59`
- Test: `test/suite/test_cc12.py`

- [ ] **Step 1: Replace the full-removal pass**

Replace `remove_ranges` with the following:

```python
def remove_ranges(self, cursor, nrows):
    if self.remove_all:
        self.session.begin_transaction()
        for key in range(nrows):
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction(
            "commit_timestamp=" + self.timestamp_str(nrows + 1))
        return

    for start in range(0, nrows, 20):
        self.session.begin_transaction()
        for key in range(start, min(start + 10, nrows)):
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction(
            "commit_timestamp=" + self.timestamp_str(nrows + 1))
```

Update the call site to:

```python
self.remove_ranges(cursor, nrows)
```

- [ ] **Step 2: Run the focused test**

Run from the build directory:

```bash
WT_BUILDDIR="$PWD" PYTHONPATH="$PWD/lang/python" python3 ../test/suite/run.py test_cc12
```

Expected: both the partial and full scenarios pass.

- [ ] **Step 3: Commit the simplification**

```bash
git add test/suite/test_cc12.py
git commit -m "Simplify checkpoint page removal test"
```
