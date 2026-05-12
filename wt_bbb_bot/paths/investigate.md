# Investigate Path

Deep-dive root cause investigation. Picks up where @paths/triage.md left off.

**Core rule:** Only assert what you fetched. Unknown = record as "unknown." Never fill
a gap with an assumption. When in doubt, do less and say more.

---

## Context from triage

These fields must be populated (from the triage handoff) before starting:

| Field | |
|---|---|
| Ticket | `WT-XXXXX` — summary |
| Task ID(s) | |
| Last-good SHA / First-bad SHA | |
| First error line | |
| Stack trace | |
| Failure type | |
| Subsystem | (may be "unknown") |
| Min repro iterations | 10 / 20 / 30 |
| CI blocker | |
| Siblings | |
| Prior investigation | |

---

## Step 1: Git History

→ **@skills/github/SKILL.md** — use the last-good/first-bad SHAs from triage context and the failing function or assertion text from the triage error line.

**1a — Commits touching the relevant subsystem between good and bad builds:**

```
mcp__devprod-mcp-gateway__git_log(
  from="<last_good_sha>",
  to="<first_bad_sha>",
  path="src/<subsystem>/"
)
```

If the subsystem is not yet known, omit `path` and scan the full window, then filter
manually for commits touching files related to the failure signal.

**1b — Blame the exact line from the stack trace:**

When triage context gave a `file:line`, find when that line last changed:

```
mcp__devprod-mcp-gateway__git_blame(file="src/<path>.c", line=<N>)
mcp__devprod-mcp-gateway__git_show(sha="<sha from blame>")
```

Look at the diff for: does the change touch the assertion or invariant that failed?

**1c — Search commits by assertion text or ticket key:**

```
mcp__devprod-mcp-gateway__git_search(query="<assertion text or WT-XXXXX>")
```

Useful when the assertion text is distinctive or you have a suspect ticket from
sibling BF comments.

**1d — Diff between good and bad builds (if window is small):**

```
mcp__devprod-mcp-gateway__git_diff(from="<last_good_sha>", to="<first_bad_sha>")
```

Use only when the window contains few commits — otherwise the output is too large to
reason about. Filter to the subsystem path if possible.

**Record for each candidate commit:**

| Field | Extract |
|---|---|
| Commit SHA | |
| Author / date | |
| Summary | one-line message |
| Files changed | relevant to the failing subsystem? yes / no |
| Linked ticket | WT-XXXXX or SERVER-XXXXX from commit message |

**Decision:** If a commit lands within the failure window and touches the failing file
or function, flag it as a suspect. State it as a fact:
- "Commit `abc1234` (WT-XXXXX, 2026-05-07) modified `src/txn/txn.c` one day before
  the first failure."

Do not conclude it caused the bug — that belongs in the working theory.
If no candidate commit is found, record "no suspect commit identified."

---

## Step 2: Codebase Lookup

Skip this step if triage context has no concrete signal (no assertion text, no function
name, no `file:line`). Record as unknown and proceed to Output.

**2a — Find the assertion or function in source:**

```bash
grep -rn "<assertion text>" src/
grep -rn "<function_name>" src/ --include="*.c" --include="*.h"
```

Record the file path and line number. Quote the assertion and the 5 lines above it
verbatim — do not interpret intent.

**2b — Identify the subsystem:**

| Prefix / path | Subsystem |
|---|---|
| `__wt_page_*`, `__wt_btree_*`, `src/btree/` | B-tree / cursor |
| `__wt_txn_*`, `src/txn/` | Transaction / timestamp |
| `__wt_evict_*`, `src/evict/` | Eviction / cache |
| `__wt_ckpt_*`, `src/checkpoint/` | Checkpoint |
| `__wt_log_*`, `src/log/` | Durability / logging |
| `__wt_rts_*`, `src/rollback_to_stable/` | Rollback-to-stable |
| `src/block_disagg/`, `src/tiered/` | Disaggregated / tiered storage |
| `test/suite/` Python `AssertionError` | API or functional test |

If none match: subsystem = "unknown."

**2c — Search for prior tickets:**

→ **@skills/jira/SKILL.md**:

```
mcp__devprod-mcp-gateway__jira_search_issues(
  jql="project = WT AND text ~ \"<assertion text>\" ORDER BY created DESC"
)
```

Record any prior tickets, their status, and whether a fix commit exists.

---

## Step 3: Reproduction Attempt

Attempt reproduction if **all** of the following hold:
1. Triage context yielded a concrete error signal (assertion text, test name, or test command).
2. Step 2 identified the subsystem (not "unknown").
3. The failure is not already explained by a known fix commit (which would make repro unnecessary).

If any condition is not met, skip this step and record "Reproduction: skipped — `<reason>`" in the Output.

**When writing a Python reproducer test — look at existing tests first.**

Before deriving the scenario setup from first principles, always spawn an Explore agent to find analogous tests in `test/suite/`:

```
Agent(subagent_type="Explore", prompt="In test/suite/, find tests that do X (e.g. write as follower, use prepared transactions in disagg mode, inline step-down/step-up). Show the key setup code for 2-3 representative examples. Search breadth: very thorough.")
```

This prevents hours of reasoning about undocumented invariants that are already encoded in working tests. If other tests do it, copy their pattern exactly.

**Spawn a sub-agent to run the repro** — do not run it inline.

Prompt the sub-agent with:
- The ticket key and one-line failure summary.
- The exact test name and command from triage context.
- The build variant from triage context (e.g. ASan, debug, release).
- The suspect commit / code location from Steps 1–2 (for context only — the sub-agent does not fix anything).
- Instruction to follow `@paths/build.md`.
- The minimum iteration count from triage context.

**The sub-agent must:**
1. Build the matching variant if the build directory does not exist.
2. Run the test at the minimum iteration count.
3. Return the reproduction output block from `@paths/build.md` (mode, command, config, build variant, workers, result, failure rate, first failure log path).

**Record the result verbatim in the Output.** Do not interpret "no failure in N runs" as "fixed" unless the minimum iteration count was met.

---

## Output

Populate every field. Write "unknown" or "insufficient data" rather than omitting a
field. This step is read-only — do not post to Jira or modify any external state.

---

### Jira context
- **Ticket:** `WT-XXXXX` — `<summary>`
- **Status / Priority / Assignee:** `<values>`
- **CAUSES links:** `<list, or none>`
- **Prior investigation:** `<yes — summary | no | partial — summary>`

### Occurrence analysis
- **Total failures:** N over X days across Y variants
- **Failure pattern:** `<evenly spread / burst / single occurrence>`
- **Variants:** `<list>`
- **CI blocker:** `<yes / no / unknown>`
- **Siblings:** `<list, or none>`

### Log evidence
- **Test:** `<name and file>`
- **First error (exact):** `"<quoted line>"`
- **Stack trace:** `<top frames, or "unavailable">`
- **`wiredtiger_open` config:** `<value, or "not found in log">`
- **Build variant:** `<value, or unknown>`

### Git history
- **Suspect commit:** `<SHA — author, date, summary, or "none identified">`
- **Linked ticket:** `<WT-XXXXX or SERVER-XXXXX from commit message, or none>`
- **In failure window:** `<yes / no / unverified>`

### Codebase
- **Subsystem:** `<name, or unknown>`
- **Assertion location:** `<file:line, or unavailable>`
- **Prior tickets:** `<list with status, or "none found">`

### Reproduction
- **Result:** `reproduced | not reproduced | inconclusive | skipped — <reason>`
- **Command:** `<exact command run>`
- **Build variant:** `<value>`
- **Iterations run:** N
- **Failure rate:** X/N
- **First failure log:** `<path, or "n/a">`

### Unknowns
- ...

**Before writing any field as "unknown", confirm you have exhausted these sources:**

| Source | Done? | Notes |
|---|---|---|
| Jira description + comments (via triage) | | |
| Evergreen log (via triage) | | |
| Build Baron failure group (via triage) | | |
| Sibling BF tickets (via triage) | | |
| Source code for the failing function / assertion (Step 2) | | |
| Git history in the failure window (Step 1) | | |
| Local build + test run to observe actual error output | | |

Only after ticking all applicable rows may a field be recorded as "unknown". If a source
is unavailable (e.g., 401 on Evergreen, macOS-only failure), record *why* it is
unavailable — "unknown" alone is not an acceptable entry when a source was never attempted.

**Local build rule:** You MUST attempt local reproduction before writing "unknown".
Steps in order:

1. Build WiredTiger (`cd build && ninja` — incremental if build dir exists).
2. Run the exact failing test: `python3 ../test/suite/run.py <test> -j1`
   with any hooks the CI used (e.g. `--hook timestamp`).
3. If the test doesn't fail on the first run, run more iterations up to the
   minimum count from triage context. Use `--repeat N` or a loop.
4. If the failure requires a platform that cannot be reproduced locally
   (e.g., macOS-only), write a *targeted* reproducer instead — a minimal
   Python script that directly exercises the hypothesis.
   Run that script and record the exact output.
5. Only after steps 1–4 are exhausted may a field be written as "unknown",
   and only with an explicit record of what was run, how many iterations,
   and what the output was.

### Working theory
*Only write this if log and code evidence directly support it. Otherwise write:
"Insufficient evidence — see unknowns."*

**Confidence:** Low / Medium / High / Very High
**What would confirm or refute this:** `<one specific thing>`

### Recommended fix

**Rule:** Only write this section if confidence is Medium or High AND you have read the
relevant source file(s) in this session. "Read" means you fetched the actual lines via
Bash grep, Read, or an Explore agent — not that you inferred the content from logs or
prior knowledge.

If you have not read the code:
- Write: `"Not proposed — relevant source not read. Read <file> before proposing a fix."`
- Do NOT write a vague directive like "fix the timestamp lifecycle bug in hook_timestamp.py".

If you have read the code, write:
- **File:** `<path:line range>`
- **Change:** one or two sentences — what specifically changes and why it fixes the violated invariant
- **Risk:** one clause on regression risk

**Fix confidence is capped by reproduction:**

| Reproduction status | Max fix confidence |
|---|---|
| Original failing test reproduced AND fix verified (failure gone after patch) | Very High |
| Scenario reproduced via targeted test/reproducer AND fix verified | High |
| Reproduced (any form) but fix not yet applied/tested | Medium |
| Not reproduced (timing, seeds, env) — fix proposed from source only | Medium |
| Cannot reproduce at all | Low |

If you propose a fix without a verified reproducer, say so explicitly:
> "Fix proposed from source analysis. Confidence is Medium until a reproduction confirms
> the fix eliminates the assertion."

Do not report High fix confidence on a change you have not tested against a real failure.

### Next action

Pick exactly one:

- **Reproduced — needs fix** — Step 3 reproduced the failure; root cause is identified: `@paths/build.md` Step 5 (fix proposal) and Step 6 (verification)
- **Reproduced — root cause unclear** — failure reproduced but mechanism not yet understood: continue source investigation before proposing a fix
- **Not reproduced** — Step 3 met the minimum iteration count with zero failures; failure may be environment-specific or already fixed: note iteration count and build variant, flag for CI monitoring
- **Repro skipped — needs local repro** — Step 3 preconditions were not met; manual repro required: `@paths/build.md`
- **Needs data inspection** — failure points to persisted state or corruption: `@skills/wt-cli/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines showing OOM / disk / agent crash>`
- **Already fixed** — only use this when ALL of the following are true: (1) a specific fix commit or ticket is identified by name in the source or git log, AND (2) the fix has been verified by running the test at ≥ minimum iterations from triage context. A single passing run, or source-code inspection alone, is not sufficient evidence.
- **Insufficient data** — `<what is missing and how to get it>`
