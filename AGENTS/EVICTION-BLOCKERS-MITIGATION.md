# Eviction Blockers Mitigation Techniques

These mitigations target repeated selection of non-evictable pages under the **worker-direct model** (workers scan LRU regions directly, no separate eviction server).

## Techniques, Pros, and Cons

| Technique | Pros | Cons |
|---|---|---|
| Separate LRU queues by property (`all`, `clean`, `dirty`, `updates`, `urgent`) | Fewer wasted checks, easier worker specialization, better policy control under pressure | Higher bookkeeping cost, multi-queue membership churn, more lock coordination |
| Skip structures (tree-level skip pointers / per-tree jump index) | Faster scans through known non-evictable tree regions | Harder correctness under concurrent insert/remove, can become metadata hotspot |
| Cool-down lists (reason-coded, 1+ lists) | Strong reduction in repeated retries for long-lived blockers | Starvation risk if reactivation rules are weak; requires bounded retry policy |
| Time-based cool-down revisit (full or partial sweeps) | Predictable background re-check cadence, simple implementation | Can recheck too early or too late for dynamic workloads |
| Event-driven cool-down revisit (checkpoint done, materialization frontier moved, timestamp advanced) | Rechecks happen when conditions actually change, very efficient for long-lived blockers | Requires reliable event wiring and fallback polling/sweep |
| Position markers/dummy anchors in LRU lists | Resume scans cheaply, preserve progress, support controlled reinsertion near anchor points | Marker invalidation/fragmentation under heavy mutation; needs robust marker lifecycle rules |
| Reason-code negative cache (`last_block_reason`, `retry_after`) per page/tree | Prevents immediate reselection of known blocked items, improves scan quality | Stale metadata can hide newly-eligible pages unless TTL/epoch handling is careful |
| Two-stage selection (cheap prefilter, then expensive checks) | Lowers CPU per candidate, keeps expensive checks for likely-evictable pages | False positives still incur second-stage cost |
| Per-tree eligibility/debt scheduler | Reduces hammering on blocked trees, improves fairness across trees | More policy complexity and tuning burden |
| Adaptive scan budgets and early-give-up thresholds | Bounds worst-case scan cost in candidate deserts | Can miss newly-eligible pages if thresholds are too aggressive |
| Batch claim plus small worker-local worksets | Lower shared-lock overhead and better locality | Batches can go stale and reduce global fairness |
| Probabilistic probe lane (random sampling) | Escapes deterministic bad regions and avoids repeat thrashing | Less predictable eviction order |
| Pressure-mode policies with hysteresis (normal vs hard) | Enables conservative behavior normally and aggressive bypass under pressure | Poor hysteresis tuning can cause oscillation |

## Notes on Position Markers

Position markers can help if they are sparse and epoch-tagged:
- Use markers to resume scanning without re-walking known bad regions.
- Allow reinserting pages/ranges relative to markers for controlled retry order.
- Invalidate or relocate markers on heavy list reshaping to avoid stale anchors.

## Adopted approach

**Cooldown regions embedded in LRU lists** are now part of the core design (see [WORK-QUEUES-AND-WORKERS.md](WORK-QUEUES-AND-WORKERS.md#cooldown-mechanics)). Five cooldown tiers (CD-transient, CD1, CD2, CD3, CD-long) are embedded as regions within each LTAILQ list, separated by sentinel pairs. Pages escalate through tiers based on failure duration. Workers scan cooldown regions using atomic timers (see [Cooldown scanning by workers](EVICTION-SCANNING.md#cooldown-scanning-by-workers)).

Additional techniques from this list that may be adopted later:
1. Event-driven reactivation from CD-long (e.g., on checkpoint completion).
2. Two-stage prefilter in the selection path.
3. Per-tree debt/eligibility scheduling.

## Discussion: Marker Horizons and Failure Counters

- **What works well:** `normal -> short -> medium -> long` marker regions let workers/server spend most effort on likely-evictable pages while still revisiting blocked pages.
- **Failure-counter pushdown is useful:** repeated failures should move a page to longer-horizon regions, but it should be **reason-aware**, not just count-based.
- **Key guardrail:** classify by blocker reason first (hazard/lock = short, checkpoint/materialization/no-ts races = long), then apply failure-count backoff inside that class.
- **Scanning policy:** bias heavily to normal (for example 80-90%), then short, then medium/long probabilistic samples; under hard pressure, temporarily increase medium/long sampling.
- **Event promotion is critical:** on checkpoint completion, stable timestamp advance, materialization frontier advance, promote matching long/medium regions back toward normal immediately.
- **Starvation protection:** enforce max-age or max-deferral so nothing stays in long forever; periodically force a bounded sweep.
- **Counter behavior:** saturating per-page failure counter with decay on success/time avoids permanent penalization from old transient failures.
- **Main risks:** marker/list maintenance complexity under concurrency, lock contention around marker insertion, and misclassification causing delayed eviction of now-eligible pages.
- **For the worker-direct model:** workers scan LRU regions and cooldown regions of their own list directly (no separate eviction server). Cooldown scanning uses per-list atomic timers to prevent multiple workers from scanning the same tier of the same list concurrently.

## Design Note: Event-Driven Pullback from Long-Term Region

Combined with event-driven cooldown lists, marker horizons can quickly pull a subset of pages out of the long-term region when blocker conditions change.

- Use reason-tagged long-term segments (for example, checkpoint-related, materialization-related, visibility-related).
- On matching events (checkpoint completion, stable timestamp advance, materialization frontier advance), promote only the affected subset toward `normal`/`short` regions.
- Apply bounded pull size per event to avoid flooding normal lanes.
- Keep a periodic low-rate fallback sweep so pages are still reconsidered if an event is missed.

This preserves low steady-state scan cost while improving time-to-recovery for pages previously blocked by long-lived conditions.

## Alternative for Transient Failures: MRU-Side Linked Retry Markers

An alternative transient-failure mechanism is to place linked retry markers near the MRU side and move blocked pages to the next marker (or `N`-th next marker) for delayed reconsideration.

- Treat markers as retry buckets: each failed page is relinked to a later bucket instead of being retried immediately.
- Use small hops for short/transient reasons (hazard/lock races) and larger hops when the same transient reason repeats.
- Keep failure-counter decay so old transient failures do not permanently penalize hot pages.
- Enforce max deferral/age so pages cannot be postponed indefinitely.

This can be combined with horizon markers (`normal/short/medium/long`):
- horizon markers provide coarse policy buckets;
- MRU-side linked retry markers provide fine-grained retry spacing within a bucket.

Pros:
- Near O(1) relink for retries.
- Lower repeated scan cost for pages that fail briefly.
- Smooth backoff without full cooldown-list round trips.

Cons:
- Extra pointer/list maintenance and MRU-side contention risk.
- More invariants to maintain under concurrency.
- Ordering becomes recency plus eligibility scheduling, not strict recency alone.

## Internal page policy (decided)

**This is now the chosen approach, documented in INSTRUCTIONS.md as the "Internal page policy".**

Only **leaf pages** participate in LRU lists. Internal pages follow these rules:

- Internal pages with at least one active (in-memory) child are **never placed in any LRU list**.
- When an internal page loses its **last active child** (the last child is evicted or deleted), the internal page is inserted into the relevant LRU lists at the **least-recently-used end** (head of the TAILQ) so it is evicted promptly.
- When a child page is instantiated under an internal page that is currently in LRU lists, the internal page is **removed from all LRU lists** immediately.

Rationale:
- Internal pages exist only to provide access to leaf pages below them. Promoting internal pages on every leaf access would cause severe lock contention on high-level pages.
- LRU reinsertion throttling would violate strict LRU order for internal pages.
- An internal page with active children must remain in memory; evicting it is wasteful and forces immediate re-reads.
- Once an internal page has no live children, it serves no purpose and should be evicted promptly.
