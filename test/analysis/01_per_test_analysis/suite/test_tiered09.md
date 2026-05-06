# test_tiered09 — Sequential connections with different bucket prefixes on the same bucket

**File:** `test/suite/test_tiered09.py`
**Storage mode:** Tiered
**Components under test:** bucket prefix switching across connection reopens, object naming (prefix + table-name + object-number), reading data from objects written under a different prefix, local object file cleanup between connections

## Test Cases

### `test_tiered09.test_tiered`
- **What it tests:** Verifies that a WiredTiger database can be opened sequentially with different `bucket_prefix` values pointing at the same underlying bucket, and that data written under one prefix is still readable when the connection is reopened with a different prefix. Sequence: (1) open with `bucket_prefix` (`pfx_`), create table `test_tiered09`, insert `"0"`, force flush — on dir_store checks that the object exists in the bucket under the first prefix; (2) manually remove the local object copy to force reading from the bucket; (3) reopen with `bucket_prefix1` (`pfx1_`), create a second table `test_second09`, insert into both, force flush — verifies both objects in bucket have correct prefix; (4) manually remove local copies; (5) reopen with `bucket_prefix2` (`pfx2_`) and verify both tables have the correct data despite the active prefix being different from the one used to write.
- **Components:** `src/tiered/conn_tiered.c` (bucket_prefix reconfiguration path), object lookup/read via storage_source, `local_retention=1`, tiered object file naming
- **Notes:**
  - Parametrized across all tiered storage backends.
  - `local_retention=1` used so that local cleanup is straightforward; objects are manually removed to guarantee the read-back goes through the storage source.
  - For dir_store, expected object paths are constructed as `bucket/<prefix><base><N>.wtobj` and checked with `os.path.exists`.
  - Exercises the scenario described in the comment: objects `1_<table>-00000001.wtobj`, `2_<table>-00000002.wtobj`, `1_<table>-00000003.wtobj` can all coexist and be read by a connection using prefix `2_`.
