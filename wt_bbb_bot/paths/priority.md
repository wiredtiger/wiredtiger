# Priority Path

Score a single WiredTiger ticket by urgency. This path runs after `@paths/investigate.md`
— it reads that output directly. No re-fetching.

**Core rule:** Scores come only from investigation output fields. If a field is
"unknown" or "insufficient data", treat it as neutral — do not inflate or penalise.

**Truth foundation:** The MongoDB Storage Engines Bug Priorities wiki is the live
authority on P1–P5 definitions. **Always fetch it at the start of every priority
evaluation** — definitions may have changed since this path was written. Score from
what the page says, not from memory of what it said.

```
mcp__devprod-mcp-gateway__confluence_get_page_by_title(space_key="WT", title="Storage Engines Bug Priorities")
```

If the fetch fails (auth error, network), note it explicitly and fall back to the
definitions in this file — but flag the output as "wiki unavailable, used cached
definitions."

---

## Step 0: Fetch the priority wiki

Before scoring anything:

```
mcp__devprod-mcp-gateway__confluence_get_page_by_title(space_key="WT", title="Storage Engines Bug Priorities")
```

Read the returned content and use it as the authoritative P1–P5 definition for this
session. If the definitions differ from what is written in this file, the wiki wins.
Record: "Wiki fetched — version `<updated date from response>`" or "Wiki unavailable —
falling back to cached definitions."

---

## Input

The investigation output from `@paths/investigate.md` for the ticket, containing:

- Jira context (priority, assignee)
- Occurrence analysis (total failures, failures last 7d, variants, CI blocker)
- Log evidence (failure type from first error / stack trace)
- Git history (suspect commit)
- Working theory (root cause origin, confidence)

---

## Step 1: Map investigation output fields to score signals

First, read the `### Structured fields` block in the investigation output. These are the authoritative machine-readable values — use them directly without re-parsing prose sections:

| Score signal | Structured field |
|---|---|
| CI blocker | `ci_blocker` |
| Distinct variants | `variants` |
| Total failures | `total_failures` |
| Failures last 7 days | `failures_last_7d` |
| Suspect commit | `suspect_commit` |
| Reproduction result | `reproduction_result` |
| Working theory confidence | `working_theory_confidence` |

For signals not in structured fields (Jira priority, failure type, assignee, age), read the relevant prose sections as before.

---

## Step 2: Score each ticket

Assign a **priority score from 0–100** anchored to the P1–P5 definitions below. Use
the Jira priority as the starting bracket, then adjust up or down within that bracket
based on the remaining signals.

### Anchor: Jira priority bracket

| Jira priority | Score range | Meaning |
|---|---|---|
| P1 — Blocker | 85–100 | Needs immediate attention; release may be blocked |
| P2 — Critical | 65–84 | Needs urgent attention; other teams may be blocked |
| P3 — Major | 35–64 | Important but not urgent; queue for normal triage |
| P4 — Minor | 15–34 | Non-critical; uncommon conditions |
| P5 — Trivial | 0–14 | Cosmetic; no functional impact |
| Unset / unknown | Neutral — use failure type to infer bracket | |

If the Jira priority is **not set**, infer the bracket from the failure type and
recurrence in Step 2a, then score within that inferred bracket.

**Skepticism rule: default to P3. Raise to P1 or P2 only when the evidence clearly
meets the bar below. When in doubt, stay lower.**

**User-impact rule:** P1 requires both data corruption/loss AND regular recurrence. A
failure in a feature not yet deployed to users (e.g. disaggregated/layered storage,
tiered storage, experimental configs) cannot be P1 regardless of severity — cap at P2.

---

### Step 2a: Failure type + recurrence → bracket

P1 and P2 are reserved for data correctness failures. Recurrence is what separates them.

**P1 — data corruption or loss, happening regularly:**

Both conditions must hold: (1) the failure indicates data corruption or loss, AND (2)
it is occurring frequently (multiple times per week or more). A single occurrence of a
corruption failure is P2, not P1.

Failure types that qualify as data corruption/loss:
- Snapshot-isolation key/value mismatch: `"snapshot-isolation: ... expected <X> found <Y>"`
- `test/format` or `test/checkpoint` indicating a data mismatch after recovery
- `random_abort`, `random_directio`, or `timestamp_abort` failures

**P2 — data corruption or loss, occurring infrequently:**

Same failure types as P1, but low recurrence (rare, single occurrence, or isolated
burst). Still serious, but not an immediate fire.

Also P2:
- Any assertion failure relating to timestamps, txnids, or updates
- A BF where **another team outside Storage Engines is waiting on WT to unblock them**
  (e.g. the Server team cannot proceed because of a WT issue) — regardless of failure type.

**P3 — everything else by default:**

Unless there is clear evidence of data corruption/loss, assume P3. This includes:
- Cache stuck failures
- Most `test/csuite` failures
- Frequently hitting assertion failures that do not imply data correctness issues
- Python/other tests where WiredTiger statistics are out of expected range
- Test failures indicating slowness (task timeout, performance regression)
- Test failure with unknown cause — e.g. "Status 137 unknown reason"
- Coverity failures on new code
- Most Python test failures

**P4:**
- Uncommon or rarely-used configuration failures
- Compiler warnings

**P5:**
- Typos in log messages, documentation, or UI strings
- Formatting issues with no functional impact

**ASAN / memory safety:**
- ASAN non-leak (use-after-free, buffer overflow, heap corruption) — default P3; raise
  to P2 only if the faulting component is on the critical data path (`src/txn/`,
  `src/btree/`, `src/evict/`, `src/checkpoint/`, `src/block/`, `src/log/`) AND the
  failure recurs regularly.
- ASAN memory leak — P3/P4; real defect but rarely urgent.

---

### Step 2b: CI blocker — adjust within bracket

If the investigation recorded CI blocker = yes (test consistently failing on trunk or a
release branch, making CI red):
- This is an independent urgency signal — it means developers can't trust CI results,
  which masks new regressions.
- Push the score to the top of its current bracket.
- Do NOT automatically raise the bracket — a CI-red P3 test stays P3, but scores at
  the high end of P3.
- **Exception:** if an external team is waiting on WT to fix the failure before they
  can proceed, that raises the bracket to P2 per the wiki definition.

---

### Step 2c: Root cause origin — adjust within bracket

- **Internal WT bug** (`src/` code) — the failure reflects a production code defect;
  no adjustment needed (already reflected in failure type).
- **Suspect commit identified** — a regression with a specific commit to bisect is
  more actionable; nudge score up 5 points within the bracket.
- **Test-only or infra** — the failure is in test scaffolding or the CI environment,
  not production code. Lower the score by 10–15 points. Do not raise to P1/P2 on
  infra evidence alone.
- **Unknown** — treat neutrally.

---

### Step 2d: Recurrence — adjust within bracket

- **Hitting CI every day or multiple times a day** — actively noisy; nudge up 5 points.
- **A few times a week** — noisy but not a daily blocker; no adjustment.
- **Not fired recently** — may already be fixed or isolated; nudge down 5 points.

---

### Step 2e: Other modifiers

- **Blast radius (many variants)** — failure across platforms, sanitizers, and release
  builds simultaneously suggests a deep correctness issue; nudge up 5 points.

---

## Step 3: Output

### Score

**WT-XXXXX — `<summary>`**
**Score: N/100 — `<P1-Critical / P2-High / P3-Medium / P4-Low / P5-Minimal>`**

Label mapping:

| Score | Label |
|---|---|
| 85–100 | P1-Critical |
| 65–84 | P2-High |
| 35–64 | P3-Medium |
| 15–34 | P4-Low |
| 0–14 | P5-Minimal |

### Rationale

One short paragraph: which wiki-defined signals drove the bracket, and which modifiers
shifted the score within it. Name the specific failure type category from Step 2a if
the failure type was the deciding factor.

### Next action

The action the investigation already recommended — restate it here with the score as
context for how urgently it should be acted on.
