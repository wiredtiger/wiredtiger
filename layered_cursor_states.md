
## Structured Inventory of Layered Cursor Subsystem State

Based on thorough examination of the codebase, here is a complete accounting of all data structures, fields, and macros holding state on behalf of layered cursors and the layered subsystem.

### Per-cursor fields

*`dhandle` (WT\_DATA\_HANDLE)*\*

* Semantic: Reference to the WT\_LAYERED\_TABLE data handle; anchors the cursor to its table's metadata.
* Set: `src/cursor/cur_layered.c:2987` (initialized in open function)
* Read: `src/cursor/cur_layered.c:85` (via cast to get collator), `src/cursor/cur_layered.c:269,310` (to access layered table metadata)
* Lifecycle: Acquired during cursor open; released on close via `__wt_cursor_dhandle_decr_use()` at line 3021\.

*`current_cursor` (WT\_CURSOR)*\*

* Semantic: Points to the active cursor (either ingest or stable) being used for iteration; swapped during state transitions.
* Set: `src/cursor/cur_layered.c:197` (cleared on close), `src/cursor/cur_layered.c:468` (reassigned during stable reopening), logic around line 334 \- 375 (adjusted in `__clayered_adjust_state`)
* Read: `src/cursor/cur_layered.c:334,442,451,459,461` (iteration logic decides which cursor is active)
* Lifecycle: Managed per operation; can be NULL or point to one of the two constituent cursors.

*`ingest_cursor` (WT\_CURSOR)*\*

* Semantic: Memory-resident B-tree cursor on the ingest-side (insertion) table; used for fresh writes.
* Set: `src/cursor/cur_layered.c:246` (opened in `__clayered_open_cursors`), cleared at line 200
* Read: `src/cursor/cur_layered.c:136,198,325,334` (checked for NULL, used for fallback iteration)
* Notes: Writable; follower may skip opening it if leader already has durable stable data.

*`stable_cursor` (WT\_CURSOR)*\*

* Semantic: Read-only B-tree cursor on the stable/durable table (may use disaggregated block manager); points to committed state.
* Set: `src/cursor/cur_layered.c:246` (opened via `__clayered_open_stable*`), closed/reopened at lines 426 \- 480
* Read: `src/cursor/cur_layered.c:137 - 138,154 - 155,202,459,468` (status checks, role-conditional visibility)
* Leader behavior: Leader asserts stable is not read-only (line 155); follower may see read-only stable if on disaggregated storage.

`snapshot_gen` (uint64\_t)

* Semantic: Snapshot generation counter captured at last state check; used to detect snapshot invalidation and force re-evaluation.
* Set: `src/cursor/cur_layered.c:587` (in `__clayered_adjust_state`)
* Read: `src/cursor/cur_layered.c:402,1284` (compared to current snapshot gen via `__wt_session_gen(session, WT_GEN_HAS_SNAPSHOT)`)
* Purpose: Detects when transaction isolation changed; triggers reopen of cursors to ensure correct visibility.

`read_timestamp` (uint64\_t)

* Semantic: Read timestamp from the transaction's shared state; tracks what timestamp the cursor must see.
* Set: `src/cursor/cur_layered.c:588` (from `txn_shared->read_timestamp` or `WT_TS_NONE`)
* Read: `src/cursor/cur_layered.c:1285,1294` (compared before/after operations to detect timestamp changes)
* Lifecycle: Updated on each state adjustment; mirrors the session transaction's read timestamp.

`checkpoint_meta_lsn` (uint64\_t)

* Semantic: LSN of the last checkpoint metadata; follower uses this to determine if stable table has advanced.
* Set: `src/cursor/cur_layered.c:582` (from `disaggregated_storage.last_checkpoint_meta_lsn` on non-leader)
* Read: `src/cursor/cur_layered.c:138,520` (checked to decide if stable cursor reopening needed)
* Leader-specific: Always set to `WT_DISAGG_LSN_NONE` on leader (line 512).

`leader` (bool)

* Semantic: Cached replica role (leader vs. follower) from connection-level state; controls read/write vs. read-only behavior.
* Set: `src/cursor/cur_layered.c:581` (from `conn->layered_table_manager.leader`)
* Read: `src/cursor/cur_layered.c:152 - 153,341,505,612,675,1135,1864` (guards write-only ops, stable cursor writability)
* Role semantics: Leader writes to ingest; non-leader reads both cursors, may skip ingest if stable is fresh.

`next_random_seed` (uint64\_t)

* Semantic: Seed value for random cursor iteration; 0 if not configured.
* Set: `src/cursor/cur_layered.c:3003` (from config `next_random_seed`)
* Read: `src/cursor/cur_layered.c:228` (passed to `__wt_random_init_seed` on constituent cursors)
* Lifecycle: Read-only after cursor open.

`next_random_sample_size` (u\_int)

* Semantic: Sample size for random walk (percentage or absolute count); 0 \= default.
* Set: `src/cursor/cur_layered.c:3006` (from config `next_random_sample_size`)
* Read: `src/cursor/cur_layered.c:233` (assigned to constituent cursor's `cbt->next_random_sample_size`)
* Lifecycle: Configuration parameter; stable after open.

---

### Per-cursor flag bits

| Flag | Meaning | Set | Tested |
| ----- | ----- | ----- | ----- |
| `WT_CLAYERED_ACTIVE` | Session has incremented cursor count; cursor is in use in iteration flow. | `cur_layered.c:148` | `cur_layered.c:141,172` |
| `WT_CLAYERED_ITERATE_NEXT` | Forward (next) iteration is active; tracks iteration direction for merge logic. | (implicitly via `__clayered_iterate`) | `cur_layered.c:1197,1231` |
| `WT_CLAYERED_ITERATE_PREV` | Backward (prev) iteration is active; mutually exclusive with ITERATE\_NEXT. | (implicitly via `__clayered_iterate`) | `cur_layered.c:1197,1236` |
| `WT_CLAYERED_RANDOM` | Cursor configured for random walk; disables normal next/prev, uses next\_random instead. | `cur_layered.c:2998` | `cur_layered.c:252,364,612` |
| `WT_CLAYERED_READ_STABLE` | Operation requires stable cursor (e.g., search on stable). Set temporarily per call. | `cur_layered.c:121` | `cur_layered.c:636,1716,1722` |

Flag persistence: `WT_CLAYERED_ACTIVE` and `WT_CLAYERED_RANDOM` persist across constituent cursor closes (`cur_layered.c:208`); all others are cleared.

---

### Connection-level state

`WT_LAYERED_TABLE_MANAGER` (`connection.h:133 - 150`)

* `init` (bool): Indicates manager has been initialized. Set by layered subsystem startup.
* `layered_table_lock` (WT\_SPINLOCK): Guards modifications to global layered table state and the entries array. Protects structural changes to layered table registry.
* Acquired: `conn_layered_table_manager.c:64` (read dhandle type to verify)
* Used around: `conn_layered.c`, `schema_open.c` during table create/open.
* `open_layered_table_count` (uint32\_t): Count of currently open/active layered tables.
* `entries` (WT\_LAYERED\_TABLE\_MANAGER\_ENTRY):\*\* Sparsely populated array indexed by ingest btree ID. Fast lookup to check if a file ID belongs to a layered table (used when applying log records).
* `entries_allocated_bytes` (size\_t): Current allocation size for the entries array.
* `leader` (bool): Connection-level replica role (true \= leader, false \= follower). Affects cursor behavior and truncate operation paths.
* Read: `cur_layered.c:153,311,328,504,508` (every cursor state check gates read/write semantics)

`WT_LAYERED_TABLE_MANAGER_ENTRY` (`connection.h:119 - 127`)

* `ingest_id`, `stable_id` (uint32\_t): File IDs of the constituent btrees.
* *`layered_uri`, `ingest_uri`, `stable_uri` (const char):*\* Full URIs for lookup. (Note: schema.h:110 also stores these on WT\_LAYERED\_TABLE.)

`layered_drain_data` nested struct (`connection.h:1025 - 1031`)

* `threads` (WT\_THREAD\_GROUP): Worker threads for draining ingest tables during step-up.
* `queue_lock` (WT\_SPINLOCK): Protects the work queue.
* `work_queue` (TAILQ): Queue of `WT_LAYERED_DRAIN_ENTRY` items, each holding a pinned ingest dhandle. Workers dequeue and drain truncate lists.
* Set: `conn_layered_ingest.c:933,911` (queue initialized, items inserted)
* Read: `conn_layered_ingest.c:803,808,810` (dequeued by worker threads)
* `running` (bool): Drain server is active.
* `thread_count` (uint32\_t): Number of drain worker threads configured.

Server flag `WT_CONN_SERVER_LAYERED` (`connection.h:1184`)

* Set in `server_flags` bitmask; indicates layered drain server thread should run during connection open.

Disaggregated storage link: `conn->disaggregated_storage.last_checkpoint_meta_lsn` (`connection.h:261`)

* Atomic uint64\_t: LSN of last checkpoint metadata page. Followers read this (via atomic acquire) to detect when stable table has new checkpoint; triggers stable cursor reopen.
* Read: `cur_layered.c:509` (follower reads atomic)
* Lifetime: Updated by checkpoint completion; readonly to cursor code.

---

### Session-level state

No cursor-specific layered fields in `WT_SESSION_IMPL`.

Per-session state relevant to layered cursors is stored in the cursor itself (`__wt_cursor_layered`) or session transaction state (`WT_TXN_SHARED`):

* `txn_shared->read_timestamp` (`session.h:247`, indirectly):\*\* Read timestamp that layered cursor captures into `cursor->read_timestamp` on each state adjustment.
* `ncursors` (u\_int): Count of active file cursors; layered cursor increments this in `__clayered_enter` and decrements in `__clayered_leave` (to maintain consistency with btree cursor bookkeeping).
* `session->dhandle`: Temporarily set to the layered table handle during cursor operations (via `CURSOR_API_CALL` macros); layered cursor initialization stores a pointer in `clayered->dhandle`.

Truncate drain context (`session.h:249 - 253`)

* `replay_trunc_ctx`: Holds txn\_id, commit\_ts, durable\_ts during truncate replay in follower step-up. Not directly layered-cursor-specific, but participates in follower ingest drain.

---

### Schema-level state (WT\_LAYERED\_TABLE & dhandle type)

`WT_LAYERED_TABLE` (`schema.h:95 - 129`)

* `iface` (WT\_DATA\_HANDLE): Base handle. Type set to `WT_DHANDLE_TYPE_LAYERED` at `conn_dhandle.c:217`.
* `ingest_btree_id` (uint32\_t): File ID of the ingest constituent; used for manager entry lookup.
* *`collator` (WT\_COLLATOR):*\* Optional custom collator for key comparison. Layered cursor retrieves it via `__clayered_get_collator` (`cur_layered.c:83 - 86`).
* `collator_owned` (int): Whether collator must be freed on table close.
* `last_ckpt_inuse` (int64\_t): Last checkpoint generation in use; tracks ingest garbage-collection boundary.
* *`key_format`, `value_format` (const char):*\* Format strings; copied to cursor on open (`cur_layered.c:2993 - 2994`).
* *`ingest_uri`, `stable_uri` (const char):*\* URIs of constituent tables; used to open constituent cursors (`cur_layered.c:269,310`).
* `truncateqh` (TAILQ\_HEAD): Queue of truncate entries. Protected by `truncate_lock` below.
* `truncate_lock` (WT\_RWLOCK): Protects truncate list membership changes. Per-entry visibility is lock-free via `WT_TRUNCATE.committed` flag.
* Acquired: Truncate range ops during ingest drain (`conn_layered_ingest.c:735`), cursor search logic that checks truncate visibility (`cur_layered.c:765`).
* `flags` (uint8\_t):
* `WT_LAYERED_TABLE_OPEN` (0x01): Table is open and accepting cursor operations. Set at `schema_open.c:714`; cleared on drop/close at `schema_list.c:250`.

WT\_DHANDLE\_TYPE\_LAYERED usage (`dhandle.h:106`)

* Enum value in `dhandle->type`. Checked/set in:
* `conn_dhandle.c:217` (assigned on table create)
* `conn_dhandle.c:118,162,217,495,651` (case statements for type-specific dhandle operations)
* `cur_layered.c:2591` (assertion that session dhandle is layered)
* `schema_open.c:652,701` (during cursor open, verification of type)
* `conn_sweep.c:107` (skip sweep for layered types)

---

### Btree-level state

No per-btree fields mark a btree as "ingest" or "stable" constituent.

Key observation: The mapping (ingest/stable ↔ btree) is maintained by URIs stored in `WT_LAYERED_TABLE` and looked up via the manager entries array (indexed by ingest btree ID). Btrees themselves are unaware of their role.

Btree flags in `WT_BTREE` (`btree.h:336 - 344`) relevant to layered workflow:

* `WT_BTREE_DISAGGREGATED` (0x0004000u): Set on btree if it uses disaggregated block manager. Stable table may have this; ingest typically does not. Affects page loading path.
* `WT_BTREE_GARBAGE_COLLECT` (0x0008000u): Ingest btree may have this flag to enable automatic garbage collection when entries become obsolete.
* `WT_BTREE_IN_MEMORY` (0x0020000u): Ingest table is cache-resident (not durable).

Checked in layered cursor: `cur_layered.c:155` asserts stable is not read-only on leader via `F_ISSET(CUR2BT(clayered->stable_cursor), WT_BTREE_READONLY)`.

---

### Cross-system pointers

`WT_TRUNCATE` in truncate queue (`schema.h:75 - 89`)

* *`layered_table` (WT\_LAYERED\_TABLE):*\* Back-pointer to the owning layered table. Used during truncate operations to access table metadata and enqueue entries.
* Set: During range truncate initiation.
* Read: During ingest drain (`conn_layered_ingest.c:735 - 870`), cursor search logic (`cur_layered.c:765`).

`WT_TRUNCATE_INFO` (`truncate.h:12 - 28`)

* No direct pointer to layered cursor, but cursors (`start`, `stop` fields) may be layered cursors. The truncate operation delegates to `__clayered_truncate_leader` / `__clayered_truncate_follower` (lines 805, 889 in `cur_layered.c`), which operate on the layered cursor's constituent cursors.

Log replay state: `WT_SESSION_IMPL->replay_trunc_ctx` (`session.h:249 - 253`)

* Used during follower step-up to replay truncates. Connected to layered ingest drain but not a direct pointer to layered cursor.

---

### Surprises / nuances

1. Snapshot/timestamp re-checking loop (`cur_layered.c:402, 587 - 588`)
* Cursor caches `snapshot_gen` and `read_timestamp` to detect external transaction state changes. On mismatch, triggers full cursor reopen. This is a polling pattern, not a notification-based invalidation.
2. Leader/follower role caching
* Each cursor caches the connection-level `leader` flag (`cur_layered.c:505, 581`). Role changes are detected via `role_change` flag and trigger `__clayered_open_cursors` to re-open with appropriate writable/readonly semantics.
3. Read timestamp from transaction; leader reads it atomically
* Follower reads `disaggregated_storage.last_checkpoint_meta_lsn` atomically with acquire semantics (`cur_layered.c:509`) to coordinate with checkpoint; leader reads `WT_DISAGG_LSN_NONE` (line 512).
4. Cursor constituent consistency issue
* Comment at `cur_layered.c:189 - 195` warns: "Note: There is no need to close the constituent cursors if it has been already done during connection-\>close performing a close of all cursors in the session". Flag `WT_CURSTD_CONSTITUENT_DEAD` prevents double-close.
5. Truncate list is lock-free for reads during iteration
* `WT_TRUNCATE.committed` is atomic bool; cursor iteration reads without holding truncate lock (`cur_layered.c:765`). Write lock is held only for list membership changes.
6. Random cursor configuration is immutable after open
* `WT_CLAYERED_RANDOM` flag and `next_random_seed/sample_size` are set during `__clayered_open` (lines 2998, 3003, 3006\) and never changed. Constituent cursors are seeded on each open path.
7. Persistent cursor flags across constituent close
* Only `WT_CLAYERED_ACTIVE` and `WT_CLAYERED_RANDOM` survive `__clayered_close_cursors` (line 208). Iteration flags (`ITERATE_NEXT/PREV`) are always cleared on reset/close, forcing re-evaluation of merge state on next operation.
8. Collator lives in layered table, not cursor
* Unlike some cursor types that own a collator, layered cursor accesses it on-demand via `__clayered_get_collator` (line 83). Comment notes this may change if collator moves to constituent cursors in future.

---

## Summary Table of Key References

| Concept | File:Line | Purpose |
| ----- | ----- | ----- |
| Cursor struct definition | `cursor.h:523 - 548` | `__wt_cursor_layered` with 9 fields \+ flags |
| Manager struct | `connection.h:133 - 150` | Connection-level layered table registry and leader state |
| Drain queue | `connection.h:1025 - 1031` | Ingest table drain worker queue during step-up |
| Layered table | `schema.h:95 - 129` | Persistent metadata for layered table (URIs, collator, truncate queue) |
| Truncate entry | `schema.h:75 - 89` | Soft-delete record in layered table's truncate queue |
| Dhandle type enum | `dhandle.h:104 - 112` | `WT_DHANDLE_TYPE_LAYERED = 1` |
| Cursor enter/leave | `cur_layered.c:108 - 177` | Active state management (CLAYERED\_ACTIVE flag) |
| State adjustment | `cur_layered.c:496 - 590` | Detects snapshot/timestamp/role changes; re-opens constituents |
| Iteration merge | `cur_layered.c:1100 - 1260` | Merges ingest+stable keys using collator; sets ITERATE\_NEXT/PREV |
| Ingest drain | `conn_layered_ingest.c:796 - 920` | Drain worker dequeues and processes ingest truncate lists |
