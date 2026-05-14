# Audit Path

Validates investigation output and decides whether to loop, post, or summarize.
Called from SKILL.md after each investigation round.

## Inputs

- Ticket key (e.g. WT-XXXXX)
- Investigation output from @paths/investigate.md (full populated Output section)
- `/tmp/wt_<ticket>_investigation_start.txt` — Unix timestamp written by SKILL.md when investigation begins (post-triage)

## Task

Walk the decision tree below in order. Stop at the first failing node and return a BLOCK with exact instructions for the investigation agent. If all nodes pass, return END.

Do not investigate, fix, or post anything. Read only.

---

## Decision Tree

### ROOT: Time check

Read `/tmp/wt_<ticket>_investigation_start.txt`. Compute elapsed seconds since that timestamp.

```
elapsed > 3600s (1 hour)?
├─ YES → SUMMARIZE: compile all findings to date and post. Do not loop.
└─ NO  → NODE 1
```

---

### NODE 1: Is this a real, open bug?

Read the investigation output. Determine whether the ticket is:
- **Already fixed** — a specific fix commit has been identified AND verified at ≥ min iterations with zero failures
- **Reverted** — the commit that introduced the bug has been reverted and no regression exists
- **Real and open** — neither of the above

```
already fixed → END: post findings
reverted      → END: post findings
real and open → NODE 2
```

If the investigation has insufficient data to determine any of the three states, return:

> BLOCK: "Cannot determine bug status — [state what evidence is missing and how to obtain it]"

---

### NODE 2: Reproduction

Read `reproduction_result` from the investigation output.

```
reproduced    → NODE 3
not reproduced → BLOCK: "Retry reproduction — [state one specific config change,
                 stress variant, or iteration increase that has not been tried]"
inconclusive  → BLOCK: "Inconclusive is not a valid result — re-run reproduction
                 using synchronous & wait execution so all processes exit before
                 results are compiled. [State what specifically made it inconclusive
                 and how to resolve it]"
```

---

### NODE 3: Root cause identified?

A root cause is identified when **every claim** in the Working Theory is backed by at least one evidence ledger item (E-number).

Check both directions:
- **Forward:** for each claim in the Working Theory → does it cite ≥1 E-number? Is each claim labeled as observed (appears in stack/log/repro) or inferred (derived from evidence)?
- **Backward:** for each E-number in the ledger → is it cited in the Working Theory or explicitly parked in Unknowns?

```
all claims cite evidence AND all E-numbers accounted for → NODE 4
otherwise → BLOCK: list each violation:
  - "Claim [X] has no supporting E-number — find evidence or remove claim"
  - "E[N] ([label]) is in the ledger but not referenced — incorporate into
     Working Theory or add to Unknowns with reason"
```

If the Working Theory describes only a symptom (e.g. WT_PREPARE_CONFLICT, timeout, SIGABRT) and the writer-side or caller-side cause has not been identified:

> BLOCK: "Working Theory names symptom [X] but root cause not identified — [state what evidence or investigation step would expose the underlying trigger]"

---

### NODE 4: Fix verified?

Read the Fix + Verify section of the investigation output.

```
verified (zero failures, ≥ min iterations) → END: post findings
not verified (failures persist after fix)  → BLOCK: "Fix applied but failures
                                              persist — [first error line from
                                              verification run]. Revise the fix."
not proposed                               → BLOCK: "Root cause identified and
                                              reproduced — propose and verify a fix
                                              before posting"
inconclusive (too few instances)           → BLOCK: "Verification inconclusive —
                                              re-run with ≥ [min iterations] instances"
skipped                                    → BLOCK: "Fix+Verify was skipped —
                                              [state which gate condition was not met
                                              and how to satisfy it]"
```

---

## Outputs

### END
All nodes passed. Proceed to posting.

State which terminal branch was reached:
- `already fixed — verified`
- `reverted`
- `fix verified`

---

### BLOCK
Return to investigation agent with:
- **Node that failed:** NODE 1 / 2 / 3 / 4
- **Exact instruction:** the text from the failing branch above, filled in with specifics from the investigation output
- **Context:** any evidence items or output fields the investigation agent should focus on

---

### SUMMARIZE (1-hour timeout)
Compile everything found so far into a Jira comment regardless of which node was reached. Include:
- What was completed (triage, codebase lookup, reproduction attempts, etc.)
- What was found (evidence ledger items, working theory if present)
- Where investigation stopped and why
- What the next human step should be

Post and do not loop further.
