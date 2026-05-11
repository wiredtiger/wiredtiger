# Priority Path

Score a single WiredTiger ticket by urgency. This path runs after `@paths/investigate.md`
— it reads that output directly. No re-fetching.

**Core rule:** Scores come only from investigation output fields. If a field is
"unknown" or "insufficient data", treat it as neutral — do not inflate or penalise.

---

## Input

The investigation output from `@paths/investigate.md` for the ticket, containing:

- Jira context (priority, assignee)
- Occurrence analysis (total failures, failures last 7d, variants, blocking)
- Log evidence (failure type from first error / stack trace)
- Git history (suspect commit)
- Working theory (root cause origin, confidence)

---

## Step 1: Map investigation output fields to score signals

For each ticket, extract these values directly from the investigation output:

| Score signal | Investigation output field |
|---|---|
| Jira priority | Jira context → Status / Priority |
| Failure type | Log evidence → first error line + stack trace |
| Root cause origin | Working theory → narrative + confidence |
| Suspect commit | Git history → Suspect commit |
| Failures last 7 days | Occurrence analysis → Failures in last 7 days |
| Distinct variants | Occurrence analysis → Variants |
| Blocking trunk / release | Occurrence analysis → Blocking trunk / release |
| Assignee | Jira context → Assignee |
| Age | Jira context → ticket creation date vs today |

---

## Step 2: Score each ticket

Assign a **priority score from 0–100** by weighing the signals below. The weights are
not fixed arithmetic — use them as a relative importance guide and apply judgment when
signals conflict or are missing.

### Jira priority

P1 or P2 means someone with full context already decided this is critical. Treat it as
the strongest single signal — a P1/P2 ticket should land at or near the top regardless
of other signals.

### Failure severity

The failure type from the investigation log evidence is the second-strongest signal.
Order from most to least severe:

- **Data corruption** — `verify` failure or unexpected key/value after recovery. The
  worst outcome: persisted data is wrong and may propagate silently. Treat as critical.
- **Crash / SIGABRT / `wiredtiger_abort`** — process died. Serious but data may be intact.
- **Data mismatch** — wrong key or value observed during the test, process survived.
  Caught, but implies a correctness bug in production code.
- **ASAN non-leak** — use-after-free, buffer overflow, heap corruption. Memory safety
  violation; often precedes a crash or corruption in real workloads.
- **`WT_PANIC` / `WT_ASSERT`** — assertion in production code fired. Indicates an
  invariant the WT team considered impossible was violated.
- **Hang / task timeout** — no forward progress. Can block entire variants.
- **ASAN memory leak** — real defect but almost never urgent; rarely causes CI failures
  in the short term.
- **Infra / test-only failure** — the problem is in the test harness or CI environment,
  not in WT code. Deprioritise significantly.

### Root cause origin

From the investigation's working theory:

- **Internal WT bug** — the stack trace, assertion, or theory points to production code
  in `src/`. This matters more than the same failure in a test harness.
- **Suspect commit identified** — a regression with a specific commit to look at is
  easier to fix and more urgent than an unknown root cause.
- **Test-only or infra** — if the investigation concluded the failure is in test
  scaffolding or environment, lower the priority substantially.
- **Unknown** — treat neutrally; do not inflate or deflate.

### Recurrence

From "Failures in last 7 days" in the occurrence analysis. The pattern to look for:

- A ticket hitting CI **every day or multiple times a day** is actively blocking
  developers from merging — it belongs at the top of the queue.
- A ticket failing **a few times a week** is noisy but not a daily blocker.
- A ticket that **has not fired recently** may already be fixed on trunk or may have
  been an isolated event — verify before spending time on it.

### Blast radius

More variants affected = wider impact. A failure showing up across platforms,
sanitizer builds, and release variants simultaneously suggests a deep correctness
issue, not a configuration fluke.

### Other signals that raise urgency

- **Blocking trunk or a release branch** — gating impact is immediate.
- **Unowned** — no one is looking at it; higher urgency to assign or triage.
- **Long open without a fix or investigation comment** — age without progress is a
  signal the ticket has been missed.

---

## Step 3: Output

### Score

**WT-XXXXX — `<summary>`**
**Score: N/100 — `<Critical / High / Medium / Low / Minimal>`**

### Rationale

One short paragraph: which signals drove the score and how they combined.

### Next action

The action the investigation already recommended — restate it here with the score as
context for how urgently it should be acted on.
