# Cursor Write Operations: Scenario Gap Analysis

## Coverage Summary by API

| API | What's Tested | What's Not Tested | Key Test Files |
|-----|---------------|------------------|-----------------|
| `cursor.insert()` | Basic insert on leader/follower; duplicate key detection with overwrite=false | Insert on tombstone key; insert with old timestamp; concurrent insert conflicts; large values | test_layered03.py, test_layered41.py |
| `cursor.update()` | Basic update on leader/follower; update with timestamps | Update on stable-only key; update on tombstone; update with old timestamp; concurrent conflicts | test_layered84.py |
| `cursor.remove()` | Basic remove on follower/leader; drain scenarios; consecutive tombstone handling | Remove on stable-only key; remove on ingest-only key; remove non-existent key; re-insert after remove | test_layered27.py |
| `cursor.modify()` | Basic modify on leader/follower; modify across checkpoints; value_format S and u | Modify on stable-only key; modify on tombstone; modify on version-split key; concurrent conflicts | test_layered16.py, test_layered_modify01.py, test_layered22.py |
| `cursor.reserve()` | Reserve in all key-location states × leader/follower; rollback after reserve | Reserve+modify in same txn; reserve+update end-to-end; concurrent reserve conflict; reserve on tombstone; reserve with old timestamp | test_layered92.py, test_layered93.py |

**Note:** CONFIG.disagg has `ops.pct.modify=0` (FIXME-WT-16479) — modify is disabled in format tests entirely.

---

## Gap Analysis

### cursor.insert()

**Covered:** Basic insert on leader (→ ingest); insert on follower (→ ingest); overwrite=false duplicate detection; insert with timestamps.

**Gap 1 [HIGH]: Insert on key with tombstone in ingest**
- Scenario: Leader writes key K at T1, removes it at T2, inserts again at T3. Must clear the tombstone.
- Risk: Tombstone may not be properly cleared on re-insert.
- Suggested test: test_layered_insert01.py

**Gap 2 [HIGH]: Insert with commit_timestamp < stable_timestamp**
- Scenario: stable_timestamp=100, try to commit insert at T=50. Should fail.
- Risk: No test validates this timestamp ordering constraint on layered tables.
- Suggested test: test_layered_insert_timestamp01.py

**Gap 3 [MEDIUM]: Insert with overwrite=true on key in both btrees**
- Scenario: Key in stable (T1), ingest has newer version (T2). Insert with overwrite=true at T3.
- Risk: Should replace the ingest version; test if stable version is correctly left behind.
- Suggested test: test_layered_insert01.py

**Gap 4 [MEDIUM]: Concurrent insert conflict (WT_ROLLBACK)**
- Scenario: Two sessions insert same key at overlapping timestamps. One should get WT_ROLLBACK.
- Risk: Conflict detection with two-btree ingest may not work correctly.
- Suggested test: test_layered_conflict01.py

**Gap 5 [MEDIUM]: Insert large value triggering overflow page**
- Scenario: Insert value > 8KB in ingest btree.
- Suggested test: test_layered_overflow01.py

---

### cursor.update()

**Covered:** Basic update on leader/follower (→ ingest); update with timestamps; overwrite cursor positioning.

**Gap 1 [HIGH]: Update on key that exists only in stable**
- Scenario: Leader writes K=v1, checkpoint (→ stable), step_up, update K=v2. Code must create ingest entry with stable as base.
- Risk: Base lookup from stable may fail if not properly handled.
- Suggested test: test_layered_update_stable01.py

**Gap 2 [HIGH]: Update on key with tombstone in ingest**
- Scenario: Key in stable at T1, deleted at T2 (tombstone in ingest), updated at T3. Should fail (key is deleted) or succeed as re-insert?
- Risk: Semantics unclear; no test validates the outcome.
- Suggested test: test_layered_update_tombstone01.py

**Gap 3 [HIGH]: Update with commit_timestamp < stable_timestamp**
- Scenario: stable_timestamp=100, update at T=50. Should fail.
- Suggested test: test_layered_update_oldts01.py

**Gap 4 [MEDIUM]: Update during role transition (follower→leader) with held write lock**
- Scenario: Follower starts update txn, steps up to leader, commits. Lock semantics change between roles.
- Suggested test: test_layered_update_rolechange01.py

**Gap 5 [MEDIUM]: Concurrent update conflict**
- Scenario: Two sessions update same key in overlapping transactions. One should get WT_ROLLBACK.
- Suggested test: test_layered_conflict02.py

---

### cursor.remove()

**Covered:** Basic remove on follower/leader (tombstone → ingest); remove in drain scenarios; consecutive tombstone handling (WT-15721, WT-16085).

**Gap 1 [HIGH]: Remove on key that exists only in stable**
- Scenario: Leader writes K=v1, checkpoint, step_up, remove K. Must create tombstone in ingest.
- Risk: Tombstone isolation between stable and ingest may fail.
- Suggested test: test_layered_remove_stable01.py

**Gap 2 [HIGH]: Remove with commit_timestamp < stable_timestamp**
- Scenario: stable_timestamp=100, remove at T=50. Should fail.
- Suggested test: test_layered_remove_oldts01.py

**Gap 3 [MEDIUM]: Remove on key that exists only in ingest**
- Scenario: Follower writes K=v1 (ingest-only), removes K in later txn. Entry should be cleaned up cleanly.
- Suggested test: test_layered_remove_ingest01.py

**Gap 4 [MEDIUM]: Remove on non-existent key**
- Scenario: Remove a key that does not exist in either stable or ingest. Should return WT_NOTFOUND.
- Suggested test: test_layered_remove_missing01.py

**Gap 5 [MEDIUM]: Re-insert immediately after remove in same transaction**
- Scenario: Same txn: remove K, then insert K with new value. Remove creates tombstone; insert must clear it.
- Suggested test: test_layered_remove_insert01.py

**Gap 6 [MEDIUM]: Concurrent remove conflict**
- Scenario: Two sessions remove same key in overlapping transactions.
- Suggested test: test_layered_conflict03.py

**Gap 7 [DEFERRED]: Remove on prepared key (write-write conflict)**
- Scenario: Session A prepares write on K, session B removes K. Should conflict.
- Suggested test: test_layered_remove_prepared01.py
*(Prepared transactions not currently supported in disagg; see PT-1 through PT-5 in 08_unsupported_features.md.)*

---

### cursor.modify()

**Covered:** Basic modify on leader/follower (→ ingest delta); modify across checkpoints; value_format=S and value_format=u; consecutive modifies.

**Gap 1 [HIGH]: Modify on key that exists only in stable (base in stable, delta in ingest)**
- Scenario: Leader writes K=v1, checkpoint, step_up, modify K with delta. Must resolve delta base from stable.
- Risk: This is the critical disagg-specific path — delta must be applied on top of stable value, not a blank.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_stable01.py

**Gap 2 [HIGH]: Modify on key with tombstone in ingest**
- Scenario: Key in stable at T1, deleted at T2, modified at T3. Should return WT_NOTFOUND.
- Risk: Modify on deleted key must be detected; ingest tombstone may not be checked.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_tombstone01.py

**Gap 3 [HIGH]: Modify on key with version split across btrees**
- Scenario: Stable has K=v1. Ingest has K=v2 (newer version). Modify applies delta. Must use ingest v2 as base, not stable v1.
- Risk: Wrong base version leads to silently corrupted data.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_two_btree01.py

**Gap 4 [HIGH]: Modify with commit_timestamp < stable_timestamp**
- Scenario: stable_timestamp=100, modify at T=50. Should fail.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_oldts01.py

**Gap 5 [DEFERRED]: Modify on prepared key (write-write conflict)**
- Scenario: Session A prepares write on K, session B modifies K. Should conflict.
*(Prepared transactions not currently supported in disagg; see 08_unsupported_features.md.)*

**Gap 6 [MEDIUM]: Modify causing overflow pages**
- Scenario: Modify large raw value repeatedly, causing overflow page transitions.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_overflow01.py

**Gap 7 [MEDIUM]: Concurrent modify conflict**
- Scenario: Two sessions modify same key in overlapping transactions.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_conflict04.py

**Gap 8 [MEDIUM]: Read-your-own-writes: insert then modify in same transaction**
- Scenario: Same txn: insert K=v1, then modify K with delta. Modify must use v1 from the uncommitted insert as the base.
- *Currently blocked: `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479). These become the first tests to write once modify is re-enabled.*
- Suggested test: test_layered_modify_read_own01.py

**Gap 9 [LOW]: Re-enable ops.pct.modify in CONFIG.disagg**
- FIXME-WT-16479: Once Gaps 1-3 are covered, remove the `ops.pct.modify=0` setting in CONFIG.disagg to enable modify in format stress tests.

---

### cursor.reserve()

**Covered:** Reserve × {stable-only, ingest-only, both, missing} × {leader, follower}; rollback after reserve.

**Gap 1 [HIGH]: reserve() then modify in same transaction**
- Scenario: reserve(K), then modify(K, delta) in same txn before commit.
- Risk: Modify after reserve must respect the write lock and use the correct base.
- Suggested test: test_layered_reserve_modify01.py

**Gap 2 [HIGH]: reserve() then update in same transaction (end-to-end)**
- Scenario: reserve(K), then cursor[K]=newval, commit. Every test currently rolls back. No test verifies the write landed in the right btree.
- Risk: The whole point of reserve() is to write after it — this is untested end-to-end.
- Suggested test: test_layered_reserve_update01.py

**Gap 3 [HIGH]: reserve() with commit_timestamp < stable_timestamp**
- Scenario: stable_timestamp=100, reserve at T=50. Should fail.
- Suggested test: test_layered_reserve_oldts01.py

**Gap 4 [MEDIUM]: Concurrent reserve on same key (write-lock conflict)**
- Scenario: Two sessions reserve same key. First succeeds; second should block or return WT_ROLLBACK.
- Risk: Write-lock semantics of reserve are completely untested on layered tables.
- Suggested test: test_layered_reserve_concurrent01.py

**Gap 5 [MEDIUM]: reserve() on key with tombstone in ingest**
- Scenario: Key in stable, tombstone in ingest (logical delete). reserve() should return WT_NOTFOUND.
- Suggested test: test_layered_reserve_tombstone01.py

**Gap 6 [MEDIUM]: Concurrent reserve + remove conflict**
- Scenario: Session A reserves K, session B removes K concurrently.
- Suggested test: test_layered_reserve_conflict01.py

**Gap 7 [DEFERRED]: reserve() on prepared key**
- Scenario: Session A prepares write on K, session B reserves K. Should conflict.
- Suggested test: test_layered_reserve_prepared01.py
*(Prepared transactions not currently supported in disagg; see 08_unsupported_features.md.)*

**Gap 8 [MEDIUM]: reserve() rollback, then re-reserve same key**
- Scenario: reserve(K), rollback_txn, reserve(K) again in new txn. Lock must be released cleanly.
- Suggested test: test_layered_reserve_rollback01.py

---

## Priority-Ranked Gap List

### HIGH
1. Update on stable-only key → test_layered_update_stable01.py
2. Modify on stable-only key → test_layered_modify_stable01.py
3. Remove on stable-only key → test_layered_remove_stable01.py
4. Modify on version-split key (stable v1, ingest v2) → test_layered_modify_two_btree01.py
5. Modify on tombstone → test_layered_modify_tombstone01.py
6. Insert on tombstone (re-insert after delete) → test_layered_insert01.py
7. reserve() end-to-end (reserve + write + commit) → test_layered_reserve_update01.py
8. reserve() + modify in same txn → test_layered_reserve_modify01.py
9. Timestamp violations (commit_ts < stable_ts) for all write ops → test_layered_*_oldts01.py

### MEDIUM
10. Concurrent insert/update/remove/modify/reserve conflicts → test_layered_conflict01-04.py
11. Update on tombstone → test_layered_update_tombstone01.py
12. Remove on ingest-only key → test_layered_remove_ingest01.py
13. Remove on non-existent key → test_layered_remove_missing01.py
14. Re-insert after remove in same txn → test_layered_remove_insert01.py
15. reserve() on tombstone → test_layered_reserve_tombstone01.py
16. reserve() concurrent conflict → test_layered_reserve_concurrent01.py
17. reserve() after rollback → test_layered_reserve_rollback01.py
18. Read-your-own-writes (insert then modify in same txn) → test_layered_modify_read_own01.py
19. Modify with overflow pages → test_layered_modify_overflow01.py
20. Write ops during role transition → test_layered_*_rolechange01.py

### LOW
21. Large value inserts (overflow pages) → test_layered_overflow01.py
22. Enable ops.pct.modify in CONFIG.disagg after Gaps 1-3 are fixed

**Note:** Modify gaps (CW-2, CW-4, CW-5, etc.) are currently blocked by `ops.pct.modify=0` in `CONFIG.disagg` (FIXME-WT-16479) but remain HIGH priority for when the flag is removed.
