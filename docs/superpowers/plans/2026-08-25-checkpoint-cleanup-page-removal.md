# Checkpoint Cleanup Page Removal Regression Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a regression test proving reclaim-space checkpoint cleanup does not load a partially removed page, but does load it after page-level removal is complete.

**Architecture:** Add a focused Python test beside the existing checkpoint-cleanup tests. The test will use a logged table with reclaim-space cleanup, force deterministic leaf-page boundaries, compare deltas of the existing `checkpoint_cleanup_pages_read_reclaim_space` statistic, and reuse `test_cc_base.wait_for_cc_to_run()` for synchronization. No production code or new statistic is required.

**Tech Stack:** WiredTiger Python test suite, `wttest`, checkpoint cleanup statistics, `dist/stat_data.py` generated statistic bindings.

---

### Task 1: Add the page-removal regression test

**Files:**
- Modify: `test/suite/test_cc09.py`
- Test: `test/suite/test_cc09.py`

- [ ] **Step 1: Add a focused reclaim-space test class**

Add a class in `test/suite/test_cc09.py` with reclaim-space cleanup enabled:

```python
@wttest.skip_for_hook("tiered", "Checkpoint cleanup does not support tiered tables")
class test_cc09_page_removal(test_cc_base):
    conn_config = (
        'statistics=(all),log=(enabled=true),'
        'checkpoint_cleanup=(method=reclaim_space,file_wait_ms=0)'
    )

    def test_page_removal(self):
        uri = 'table:cc09_page_removal'
        create_params = (
            'key_format=i,value_format=S,'
            'allocation_size=512,leaf_page_max=512,internal_page_max=512'
        )
        nrows = 1000

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, 'value')
        self.session.checkpoint()
        self.reopen_conn(config=self.conn_config)
        cursor = self.session.open_cursor(uri)

        pages_before = self.get_stat(
            stat.conn.checkpoint_cleanup_pages_read_reclaim_space)

        for key in range(0, nrows // 2):
            self.session.begin_transaction()
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction()
        self.session.checkpoint()
        self.wait_for_cc_to_run()

        pages_after_partial_remove = self.get_stat(
            stat.conn.checkpoint_cleanup_pages_read_reclaim_space)
        self.assertEqual(pages_after_partial_remove - pages_before, 0)

        for key in range(nrows // 2, nrows):
            self.session.begin_transaction()
            cursor.set_key(key)
            self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction()
        self.session.checkpoint()
        self.wait_for_cc_to_run()

        pages_after_full_remove = self.get_stat(
            stat.conn.checkpoint_cleanup_pages_read_reclaim_space)
        self.assertGreater(pages_after_full_remove - pages_after_partial_remove, 0)
        cursor.close()


if __name__ == '__main__':
    test_cc09_page_removal.run()
```

Keep the test's key ranges and small leaf pages fixed so the first deletion
phase leaves live records on the target page and the second phase removes the
remaining records. Use the class's own `run()` entry point in the file's
standard entry-point pattern.

- [ ] **Step 2: Run the focused test before implementation changes**

Run:

```bash
cd build && python3 ../test/suite/run.py test_cc09
```

Expected: the new test fails if the cleanup walk loads the partially removed
page, with the first delta reported as greater than zero.

- [ ] **Step 3: Run the focused test after the test-only change**

Run:

```bash
cd build && python3 ../test/suite/run.py test_cc09
```

Expected: the existing `test_cc09` scenarios and the new page-removal test
pass, with zero reclaim-space reads after partial removal and a positive delta
after complete removal.

- [ ] **Step 4: Commit the regression test**

```bash
git add test/suite/test_cc09.py
git commit -m "WT-13076 test page removal cleanup reads"
```

The commit must include the repository's required Copilot co-author trailer.
