# random_session — Session RNG uniqueness and independence test

**Path:** `test/csuite/random_session/`
**Language:** C
**Storage mode:** General (WiredTiger opened but no data operations)
**Jira ticket:** N/A
**Components under test:** Per-session RNG initialization (`rnd_random`), `__wt_random_init_seed`, `__wt_random`

## What This Test Does
This test verifies that each WiredTiger session is seeded with an independent random number generator that produces outputs distinct from other sessions and from sequentially seeded generators. It checks both the raw random output and its distribution modulo 2048, ensuring that different seeds produce different sequences and that concurrent sessions do not share state.

## Test Scenarios / Cases

### Scenario: Sequential RNG seed divergence (`test_rng_seq`)
- **What it tests:** That two generators initialized with seeds 1 and 2 produce different output at least half the time over 100 samples (both in absolute value and modulo 2048).
- **Components:** `__wt_random_init_seed`, `__wt_random`.
- **Notes:** Verifies the seed-to-output mapping is not trivially collapsed.

### Scenario: Seed initialization spread (`test_rng_init`)
- **What it tests:** That initializing a generator with seeds 0..99 yields a different first output at least half the time between consecutive seeds.
- **Components:** `__wt_random_init_seed`.
- **Notes:** Guards against seeds mapping to identical initial states.

### Scenario: Sequential single-session uniqueness
- **What it tests:** That opening and closing a session 10 times in a row, reading one random number per session, yields different values at least half the time (both raw and modulo 2048).
- **Components:** `conn->open_session`, `WT_SESSION_IMPL::rnd_random`, `session->close`.
- **Notes:** Each session uses the per-session RNG embedded in `WT_SESSION_IMPL`.

### Scenario: Concurrent multi-session uniqueness
- **What it tests:** That 10 simultaneously open sessions each produce random numbers distinct from each other's outputs over 100 rounds, both in raw value and modulo 2048.
- **Components:** Parallel session RNG state, `__wt_random`.
- **Notes:** Guards against shared or correlated RNG state across sessions.

## LazyFS Variant
None.
