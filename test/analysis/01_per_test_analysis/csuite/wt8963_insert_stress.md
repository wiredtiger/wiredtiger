# wt8963_insert_stress — High-concurrency random insert stress test

**Path:** `test/csuite/wt8963_insert_stress/`
**Language:** C
**Storage mode:** General
**Jira ticket:** BF-24385
**Components under test:** `cursor->insert`, concurrent insert with random keys, large in-memory page images, skip-list insert path, `session->verify`

## What This Test Does
This test reproduces BF-24385 by stressing the insert path under high concurrency with large in-memory pages. It creates 110 threads that all simultaneously insert 200,000 records each with random uint64 keys (range [1, UINT32_MAX]) into a single table configured with `memory_page_image_max=50MB` and `cache_size=4GB`. All threads spin on a shared `ready_counter` barrier to start as simultaneously as possible. After all threads complete, the connection is reopened and `session->verify` is called to confirm B-tree structural integrity. The final record count is printed.

## Test Scenarios / Cases

### Scenario: Row-store concurrent random insert with large page images
- **What it tests:** That concurrent random inserts from 110 threads into a single row-store table with 50 MB page images do not cause B-tree corruption, assertion failures, or skip-list inconsistencies.
- **Components:** 110 pthreads, `cursor->insert`, `__wt_random` key generation, `memory_page_image_max=50MB`, `cache_size=4G`, `session->verify` post-run.
- **Notes:** THREAD_NUM_ITERATIONS=200,000, NUM_THREADS=110, KEY_MAX=UINT32_MAX. Threads synchronize via `ready_counter` atomic variable before starting. Column-store variant (`key_format=r`) is also supported via test options.

## LazyFS Variant
None.
