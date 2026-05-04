# Transactions and Timestamps: Scenario Gap Analysis

## Coverage Summary

**Strong areas:**
- Basic timestamp operations (set_timestamp with stable_timestamp, oldest_timestamp)
- Prepared transaction mechanics (prepare, commit with durable_timestamp, rollback)
- Prepare conflict detection (WT_PREPARE_CONFLICT on cursor operations)
- Delta page generation with prepared and committed updates
- Follower checkpoint advancement and snapshot isolation basics
- Prepared transaction rollback with reconciliation on leader
- Prepared transaction visibility/invisibility on followers during leader checkpoint

**Files covering this area:** test_layered45.py, test_layered65.py, test_layered69.py, test_layered73.py, test_layered84.py, test_layered87.py, test_layered89.py, test_layered94.py, test_layered53.py

**Hook behavior:** hook_disagg.py line 377 skips ALL tests with "rollback_to_stable" in name. test_layered87.py is the only direct RTS test (87 lines).

---

## Gap Analysis

### Timestamps

**Gap 1 [HIGH]: Read timestamp older than oldest_timestamp**
- Scenario: Session begins transaction with `read_timestamp=T` where T < oldest_timestamp.
- Why: Disagg behavior may differ from monolithic. Followers reading through disagg tables with old read_timestamp may have unexpected outcomes if old ingest versions have been GC'd.
- Suggested test: test_layered_timestamps01.py — set oldest_timestamp=100, begin_transaction(read_timestamp=50), attempt reads, verify error or WT_NOTFOUND

**Gap 2 [HIGH]: oldest_timestamp advancement and ingest garbage collection**
- Scenario: Leader inserts at ts=10,20,30. Checkpoints. Does more writes at ts=40,50. Advances oldest_timestamp to 50. Verify old ingest versions are discarded.
- Why: Core GC correctness for disagg. Ingest accumulation is a known pain point.
- Suggested test: test_layered_timestamps02.py

**Gap 3 [HIGH]: Commit timestamp < stable_timestamp (should fail)**
- Scenario: stable_timestamp=100, try commit_transaction(commit_timestamp=50). Should fail with WT_INVALID_ARGUMENT or similar.
- Why: Timestamp ordering enforcement; disagg may validate differently.
- Suggested test: test_layered_timestamps03.py

**Gap 4 [MEDIUM]: Durable timestamp vs commit timestamp in drain filtering**
- Scenario: Prepare at ts=10, commit_timestamp=20, durable_timestamp=30. Checkpoint at ts=25. Verify drain includes/excludes the data correctly based on `durable_start_ts > last_checkpoint_timestamp`.
- Why: Code in conn_layered_ingest.c:404 uses durable_start_ts as the filter — this exact boundary is not tested.
- Suggested test: Extend test_layered45.py

**Gap 5 [MEDIUM]: query_timestamp('get=all_durable') on disagg connection**
- Scenario: Write at various timestamps, prepare some. Query all_durable on both leader and follower.
- Why: Monitoring and replication checkpoint logic may depend on this value; never verified for disagg.
- Suggested test: test_layered_timestamps04.py

**Gap 6 [MEDIUM]: Query timestamp on follower after step-up**
- Scenario: Follower becomes leader. What is the state of query_timestamp('get=all_durable') and ('get=last_checkpoint') after promotion?
- Suggested test: Extend test_layered94.py

**Gap 7 [LOW]: Transactions without any set_timestamp call**
- Scenario: No stable_timestamp ever set. All writes are at arbitrary timestamps. Does drain work correctly without a reference checkpoint timestamp?
- Suggested test: test_layered_edge_cases01.py

---

### Prepared Transactions

**Note (May 2026):** Prepared transactions are not currently supported in disaggregated storage in their disagg-specific form ("No | Public Preview"). The new disagg guarantee — that prepared content is included in a checkpoint based on timestamp rules — is not yet implemented. All prepared-transaction gaps below are DEFERRED until Public Preview. See `08_unsupported_features.md` (PT-1 through PT-5) for tracking.

**Gap 8 [DEFERRED]: Prepare + checkpoint before commit — prepared data in snapshot**
- Scenario: Leader prepares at ts=10, checkpoints, prepare still unresolved. Does checkpoint snapshot include prepared data when preserve_prepared=true? Does follower see it after advancing checkpoint?
- Why: Prepared transaction visibility is critical for replication correctness.
- Suggested test: test_layered_prepared01.py

**Gap 9 [DEFERRED]: Multiple prepares in-flight during step-up**
- Scenario: Follower has multiple concurrent prepared transactions when step-up runs. Can the new leader commit/rollback each prepare? Are there edge cases with durable_timestamp assignment?
- Why: test_layered94.py covers one prepare at step-up but not multiple.
- Suggested test: Extend test_layered94.py or new test_layered_prepared02.py

**Gap 10 [DEFERRED]: Prepare + drain interaction (code has "temporary solution" comment)**
- Scenario: Leader prepares a key, let drain run. Verify ingest preserves prepared data and stable does not include it (or includes with correct marker). Then commit and verify both trees consistent.
- Why: conn_layered_ingest.c:286 comment says "This is a temporary solution. It assumes no concurrent commit/rollback of the prepared."
- Suggested test: test_layered_prepared03.py

**Gap 11 [DEFERRED]: Prepared rollback after drain has started**
- Scenario: Drain is copying ingest→stable. Meanwhile a prepared transaction is rolled back. Does drain correctly handle the rollback?
- Suggested test: test_layered_concurrency01.py

**Gap 12 [DEFERRED]: Follower cannot see uncommitted prepared data**
- Scenario: Leader prepares a key, checkpoints with preserve_prepared=true. Follower advances checkpoint. Follower reads prepared key — should get WT_PREPARE_CONFLICT or key-not-found.
- Suggested test: test_layered_prepared04.py

---

### Rollback to Stable (RTS)

**Note (May 2026):** RTS is not supported in disaggregated storage ("Never"). See `08_unsupported_features.md` (RTS-1 through RTS-5) for the revised test goals.

---

### Isolation and Concurrency

**Gap 13 [MEDIUM]: Snapshot isolation: long-running reader while writer advances**
- Scenario: Follower session A begins at read_timestamp=T1. Session B writes and advances to T2 >> T1. Session A continues reading and still sees T1 snapshot.
- Suggested test: test_layered_isolation01.py

**Gap 14 [MEDIUM]: Write conflict detection on same key**
- Scenario: Two sessions write the same key at the same timestamp. WT_ROLLBACK expected on one.
- Suggested test: test_layered_concurrency02.py

**Gap 15 [MEDIUM]: Read-your-own-writes within a transaction**
- Scenario: Txn: write key K, then read key K in same transaction before commit. Should see the written value.
- Suggested test: test_layered_isolation02.py

**Gap 16 [MEDIUM]: Concurrent readers on follower while leader is checkpointing**
- Scenario: Follower has active reader (long txn). Leader checkpoints. Follower advances checkpoint. Reader continues without seeing torn state.
- Suggested test: test_layered_concurrency03.py

**Gap 17 [MEDIUM]: Isolation level enforcement (snapshot vs read_uncommitted vs read_committed)**
- Scenario: Test each isolation level on both leader and follower. Verify uncommitted, prepared, and stable data visibility rules.
- Suggested test: test_layered_isolation03.py

**Gap 18 [MEDIUM]: Multiple concurrent prepares on same key**
- Scenario: Session A prepares key K at ts=10. Session B tries to prepare key K at ts=20. Should conflict.
- Suggested test: test_layered_concurrency04.py

---

## Priority-Ranked Gap List

### HIGH
1. Read timestamp older than oldest_timestamp → test_layered_timestamps01.py
2. oldest_timestamp advancement and ingest GC → test_layered_timestamps02.py
3. Commit timestamp < stable_timestamp → test_layered_timestamps03.py

### MEDIUM
4. Durable timestamp vs commit timestamp in drain → extend test_layered45.py
5. query_timestamp('get=all_durable') on disagg → test_layered_timestamps04.py
6. Query timestamp state after step-up → extend test_layered94.py
7. Snapshot isolation verification → test_layered_isolation01.py
8. Write conflict detection → test_layered_concurrency02.py
9. Read-your-own-writes → test_layered_isolation02.py
10. Concurrent readers during checkpoint → test_layered_concurrency03.py
11. Isolation level enforcement → test_layered_isolation03.py
12. Multiple concurrent prepares on same key → test_layered_concurrency04.py

### LOW
13. Transactions without any set_timestamp → test_layered_edge_cases01.py

### Deferred — Prepared Transactions (Target: Public Preview)
*(PT-1 through PT-5 — see `08_unsupported_features.md` for details.)*

### Unsupported (see 08_unsupported_features.md)
*RTS gaps are tracked in [08_unsupported_features.md](08_unsupported_features.md) (RTS-1 through RTS-5).*

