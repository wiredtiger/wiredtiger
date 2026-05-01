# test_stat02 — Statistics cursor configuration and clear semantics

**File:** `test/suite/test_stat02.py`
**Storage mode:** General
**Components under test:** statistics cursor, cache walk, tree walk, connection/data-source configuration

## Test Cases

### `test_stat_cursor_config.test_stat_cursor_config`
- **What it tests:** For each combination of database-level statistics configuration (`none`, `all`, `fast`) and cursor-level configuration (`empty`, `all`, `fast`, `size`), confirms that opening a statistics cursor either succeeds or raises `'database statistics configuration'` error as expected.
- **Components:** `stat.c`, `conn.c`
- **Notes:** Parameterized over file/table/complex URI × data_config × cursor_config; `size` is valid with `all` or `fast`.

### `test_stat_cursor_conn_clear.test_stat_cursor_conn_clear`
- **What it tests:** Verifies that connection-level stats marked clearable (e.g. `cursor_insert`) are zeroed on the second `all,clear` cursor open, while non-clearable stats (`cache_bytes_dirty`) remain non-zero.
- **Components:** `stat.c`, `conn.c`
- **Notes:** Single scenario; uses `ComplexDataSet`.

### `test_stat_cursor_dsrc_clear.test_stat_cursor_dsrc_clear`
- **What it tests:** Verifies that data-source `cursor_insert` is zeroed on the second `all,clear` open against the same URI.
- **Components:** `stat.c`
- **Notes:** Parameterized over file, simple table, and complex table URIs.

### `test_stat_cursor_fast.test_stat_cursor_fast`
- **What it tests:** Confirms that a `fast` statistics cursor does not traverse the btree and returns `btree_entries == 0`, while `all` does populate `btree_entries > 0`.
- **Components:** `stat.c`, `btree`
- **Notes:** Parameterized over file, simple table, complex table URIs.

### `test_stat_cursor_conn_error.test_stat_cursor_conn_error`
- **What it tests:** Confirms that specifying two conflicting statistics modes simultaneously in the connection config (e.g. `none,all`, `none,fast`, `all,fast`) raises `'Only one of'` error.
- **Components:** `conn.c`, `config.c`
- **Notes:** Iterates all 2-permutations of `['none', 'all', 'fast']`.

### `test_stat_cursor_dsrc_error.test_stat_cursor_dsrc_error`
- **What it tests:** Confirms that specifying both `all` and `fast` on a data-source statistics cursor raises `'Only one of'` error.
- **Components:** `stat.c`
- **Notes:** Parameterized over file, simple table, complex table URIs.

### `test_stat_cursor_dsrc_cache_walk.test_stat_cursor_dsrc_cache_walk`
- **What it tests:** Validates `cache_walk` and `tree_walk` configuration interactions: `cache_walk` without `tree_walk` populates `cache_state_root_size` but not `btree_entries`; combining both populates both; `all` implies both; `fast` alone populates neither. Also confirms error when statistics=none.
- **Components:** `stat.c`, `evict.c`, `btree`
- **Notes:** Uses `conn.reconfigure()` to cycle between configurations mid-test.
