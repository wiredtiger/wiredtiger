---
name: help-ticket-triage
description: Triage a WiredTiger HELP ticket end-to-end. Reads context.md for symptoms, matches against the Cache Eviction Checkpoint Playbook in the vault, extracts FTDC metrics from adjacent subfolders, and writes findings to the vault progress file. Use when investigating a HELP ticket with FTDC data.
---

# HELP Ticket Triage

## Prerequisites

- The workspace root contains a `context.md` describing the ticket background and customer-reported symptoms.
- Raw FTDC captures are in subfolders adjacent to `context.md` (one subfolder per node, containing `metrics.*` files).
- The Cache Eviction Checkpoint Playbook lives in the vault at:
  `{vault}/HELP Playbook/Cache Eviction Checkpoint Playbook.md`
  where `{vault}` is the vault path defined in the workspace rules.

## Workflow

### Step 1 — Read inputs

1. Read `context.md` from the workspace root.
2. Read the Cache Eviction Checkpoint Playbook from the vault.
3. List all subfolders in the workspace to discover FTDC files per node.

### Step 2 — Match symptoms to playbook patterns

Review every pattern in the playbook. For each one, check whether the symptoms described in `context.md` could match. Produce a shortlist of candidate patterns, each annotated with the specific symptoms that triggered the match.

### Step 3 — Discover FTDC stat keys

FTDC files encode stats under varying key paths. Before bulk extraction, load the first sample from one FTDC file and flatten it to discover the actual key names. Search the flattened keys for substrings relevant to each candidate pattern (e.g. `update`, `dirty`, `evict`, `checkpoint`, `thread-yield`, `hazard`, `pinned`, `history store`, `repl`, `flowControl`, `concurrentTransactions`).

Use this discovery snippet:

```python
python3 -c "
import sys
sys.path.insert(0, '~/.cursor/skills/perf-stat-triage/scripts')
from parse_stats_jsonl import *

path = '<FTDC_FILE>'
it = iter_samples(path, 'bson')
first = next(it)

def get_flat(obj):
    root = obj.get('wiredTiger')
    if root is None:
        root = obj.get('serverStatus', obj)
    return flatten_dict(root)

flat = get_flat(first)
for k in sorted(flat.keys()):
    kl = k.lower()
    if '<substring>' in kl:
        print(f'{k} = {flat[k]}')
" 2>/dev/null
```

### Step 4 — Extract FTDC metrics with perf-stat-triage

For each FTDC file and each candidate pattern, extract the relevant stats using the perf-stat-triage helper:

```bash
python3 ~/.cursor/skills/perf-stat-triage/scripts/parse_stats_jsonl.py \
  --file "<FTDC_FILE>" \
  --format bson \
  --stat "<stat_name_1>" \
  --stat "<stat_name_2>"
```

#### Recommended stat buckets per pattern

**Cache pressure / application-thread eviction:**
- `wiredTiger.cache.bytes allocated for updates`
- `wiredTiger.cache.tracked dirty bytes in the cache`
- `wiredTiger.cache.bytes currently in the cache`
- `wiredTiger.cache.maximum bytes configured`
- `wiredTiger.cache.eviction empty score`
- `wiredTiger.cache.eviction currently operating in aggressive mode`
- `wiredTiger.thread-yield.application thread time evicting (usecs)`
- `wiredTiger.thread-yield.application thread time waiting for cache (usecs)`
- `wiredTiger.cache.modified pages evicted by application threads`
- `wiredTiger.cache.pages evicted by application threads`
- `wiredTiger.cache.eviction worker thread active`

**Checkpoint:**
- `wiredTiger.transaction.transaction checkpoint most recent time (msecs)`
- `wiredTiger.transaction.transaction range of IDs currently pinned`
- `wiredTiger.transaction.transaction range of IDs currently pinned by a checkpoint`

**Eviction blockers:**
- `wiredTiger.cache.hazard pointer blocked page eviction`
- `wiredTiger.cache.forced eviction - pages selected count`
- `wiredTiger.cache.forced eviction - pages selected unable to be evicted count`
- `wiredTiger.cache.eviction server skips pages that are written with transactions greater than the last running`
- `wiredTiger.cache.pages queued for eviction`
- `wiredTiger.cache.pages queued for urgent eviction`
- `wiredTiger.capacity.bytes written for eviction`

**History store:**
- `wiredTiger.cache.bytes belonging to the history store table in the cache`
- `wiredTiger.cache.pages queued for urgent eviction from history store due to high dirty content`

**Write tickets / Flow Control:**
- `wiredTiger.concurrentTransactions.write.out`
- `wiredTiger.concurrentTransactions.write.available`
- `wiredTiger.concurrentTransactions.write.totalTickets`
- `wiredTiger.concurrentTransactions.write.queueLength`
- `flowControl.isLagged`
- `flowControl.isLaggedCount`
- `flowControl.isLaggedTimeMicros`
- `flowControl.targetRateLimit`

**Replication (secondaries):**
- `metrics.repl.buffer.count`
- `metrics.repl.buffer.sizeBytes`
- `metrics.repl.buffer.maxSizeBytes`
- `metrics.repl.apply.batches.totalMillis`
- `metrics.repl.network.getmores.totalMillis`

### Step 5 — Confirm or rule out each pattern

For each candidate pattern from Step 2, compare the extracted metrics against the diagnostic criteria in the playbook. Confirm a pattern when the data supports it; rule it out when it does not. Note which specific stat values led to the decision.

### Step 6 — Write findings

Write the results to the vault progress file at:
`{vault}/{TICKET}/progress.md`

Use this structure:

```markdown
# {TICKET} — Triage Progress

## Ticket Summary
- MongoDB version, topology, node names, cache size, user impact

### Timeline
| Time (UTC) | Event |
|---|---|
| ... | ... |

## Step 1 — Symptom-to-Playbook Pattern Matching
For each candidate pattern: name, matching symptoms, playbook section reference.

## Step 2 — FTDC Verification
For each node and each time window, a table of key metrics:

| Metric | First | Last | Min | Max | Trend |
|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... |

With analysis paragraphs explaining what the data shows.

## Step 3 — Root Cause Summary

### Findings
- [Most likely root cause], supported by [key counters + deltas].
- [Secondary contributor], supported by [key counters + deltas].

### Evidence
| Stat | Node | Value | Significance |
|---|---|---|---|
| ... | ... | ... | ... |

### Next Checks / Recommendations
- Numbered list of follow-up actions, fixes to check, upgrade paths.
```

## Tips

- Always read the existing progress file first (if it exists) to maintain continuity.
- Run FTDC extractions in parallel across files/nodes when the queries are independent.
- When a stat name returns MISSING, use the discovery snippet from Step 3 to find the correct key path.
- Compute ratios against `maximum bytes configured` to express cache usage as percentages.
- For secondaries, always check the WT-9575 pattern (`eviction server skips pages that are written with transactions greater than the last running`).
- For long checkpoints, check both the reconciliation time and the cleanup time.
- Note the eviction thread count (`eviction worker thread active`) — config changes may not propagate to all nodes simultaneously.
