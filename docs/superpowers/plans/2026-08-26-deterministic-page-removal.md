# Deterministic checkpoint-cleanup page-removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic partial- and full-page-removal regression coverage and remove the unreliable delete-loop change from `test_cc09.py`.

**Architecture:** Keep `test_cc09.py` focused on obsolete time-window cleanup. Add a dedicated checkpoint-cleanup test with fixed small leaf pages and explicit 10-key removal ranges. Both scenarios verify that cleanup does not read the data source after the removal checkpoint and reopen; the full scenario additionally removes the complementary ranges.

**Tech Stack:** WiredTiger Python test suite, `wttest`, `wtscenario`, generated WiredTiger statistics.

---

### Task 1: Restore deterministic `test_cc09.py` behavior

**Files:**
- Modify: `test/suite/test_cc09.py:49-95`

- [ ] **Step 1: Revert the nondeterministic delete-loop edit**

Restore the scenario name `newest_stop_durable_ts` and replace the loop that removes 1000 keys with the original single-key removal:

```python
cc_scenarios = [
    ('newest_stop_durable_ts', dict(has_delete=True, bump_oldest_ts=False)),
    ('obsolete_ts', dict(has_delete=False, bump_oldest_ts=True)),
    ('none', dict(has_delete=False, bump_oldest_ts=False)),
]
```

```python
if self.has_delete:
    self.session.begin_transaction()
    cursor.set_key(1)
    cursor.remove()
    self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(nrows + 1))
    self.session.checkpoint()
```

- [ ] **Step 2: Run the existing test before adding new coverage**

Run from the build directory:

```bash
python3 ../test/suite/run.py test_cc09
```

Expected: the existing `test_cc09` scenarios pass.

- [ ] **Step 3: Commit the restoration**

```bash
git add test/suite/test_cc09.py
git commit -m "Restore deterministic checkpoint cleanup coverage"
```

### Task 2: Add deterministic partial and full removal scenarios

**Files:**
- Modify: `src/btree/bt_sync_obsolete.c:461-470`
- Create: `test/suite/test_cc12.py`

- [ ] **Step 1: Add the focused test**

Create a test with these concrete behaviors:

```python
import wttest
from test_cc01 import test_cc_base
from wiredtiger import stat
from wtscenario import make_scenarios


@wttest.skip_for_hook("tiered", "Checkpoint cleanup does not support tiered tables")
class test_cc12(test_cc_base):
    conn_config = "statistics=(all),checkpoint_cleanup=(wait=1,file_wait_ms=0)"
    scenarios = make_scenarios([
        ("partial_remove", dict(remove_all=False)),
        ("full_remove", dict(remove_all=True)),
    ])

    def remove_ranges(self, cursor, nrows):
        for start in range(0, nrows, 20):
            self.session.begin_transaction()
            for key in range(start, min(start + 10, nrows)):
                cursor.set_key(key)
                self.assertEqual(cursor.remove(), 0)
            self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(nrows + 1))

        if self.remove_all:
            for start in range(10, nrows, 20):
                self.session.begin_transaction()
                for key in range(start, min(start + 10, nrows)):
                    cursor.set_key(key)
                    self.assertEqual(cursor.remove(), 0)
                self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(nrows + 2))

    def test_cc12(self):
        uri = "table:cc12"
        nrows = 1000
        create_params = (
            "key_format=i,value_format=S,"
            "allocation_size=512,leaf_page_max=512,internal_page_max=512")

        self.session.create(uri, create_params)
        self.populate(uri, 0, nrows, "k" * 40)
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows))
        self.session.checkpoint()
        self.reopen_conn()

        cursor = self.session.open_cursor(uri)
        self.remove_ranges(cursor, nrows)
        cursor.close()
        self.conn.set_timestamp("stable_timestamp=" + self.timestamp_str(nrows + 2))
        self.session.checkpoint()
        self.reopen_conn()
        keep_open = self.session.open_cursor(uri)

        pages_read_before = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri)
        self.wait_for_cc_to_run()
        pages_read_after = self.get_stat(stat.dsrc.checkpoint_cleanup_pages_read, uri)

        self.assertEqual(pages_read_after - pages_read_before, 0)
        keep_open.close()
```

Before running the test, make the cleanup walk use `addr.ta.newest_stop_durable_ts`
for the existing logged-table decision:

```c
if (addr.type == WT_ADDR_LEAF_NO)
    *skipp = true;
else if (addr.ta.newest_stop_durable_ts == WT_TS_NONE) {
    *skipp = !F_ISSET(S2C(session), WT_CONN_CKPT_CLEANUP_RECLAIM_SPACE) ||
      !F_ISSET(S2BT(session), WT_BTREE_LOGGED);
    if (!*skipp)
        WT_STAT_CONN_DSRC_INCR(session, checkpoint_cleanup_pages_read_reclaim_space);
}
```

Use the existing `wait_for_cc_to_run` helper rather than sleeps. The partial scenario removes every other contiguous 10-key range, while the full scenario independently removes the complementary ranges as well.

- [ ] **Step 2: Run the new test to verify the behavior**

Run:

```bash
python3 ../test/suite/run.py test_cc12
```

Expected: both scenarios pass with no data-source page reads after the removal checkpoint and reopen.

- [ ] **Step 3: Commit the focused test**

```bash
git add src/btree/bt_sync_obsolete.c test/suite/test_cc12.py
git commit -m "Add deterministic checkpoint page removal coverage"
```

### Task 3: Run the combined checkpoint-cleanup coverage

**Files:**
- Test: `test/suite/test_cc09.py`
- Test: `test/suite/test_cc12.py`

- [ ] **Step 1: Run both related suites**

Run:

```bash
python3 ../test/suite/run.py test_cc09 && python3 ../test/suite/run.py test_cc12
```

Expected: all `test_cc09` and `test_cc12` scenarios pass without changes to statistics or production code.
