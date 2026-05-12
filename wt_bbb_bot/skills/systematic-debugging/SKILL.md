---
name: systematic-debugging
description: Use when encountering any bug, test failure, or unexpected behavior, before proposing fixes.
when_to_use: encountering any bug, test failure, or unexpected behavior
source: obra/superpowers
license: MIT
---

# Systematic Debugging

## Overview

Random fixes waste time and create new bugs. Quick patches mask underlying issues.

**Core principle: ALWAYS find root cause before attempting fixes. Symptom fixes are failure.**

Violating the letter of this process is violating the spirit of debugging.

## The Iron Law

> **NO FIXES WITHOUT ROOT CAUSE INVESTIGATION FIRST**

If you haven't completed Phase 1, you cannot propose fixes.

## When to Use

Use for ANY technical issue: test failures, bugs in production, unexpected behavior, performance problems, build failures, integration issues.

Use this **especially** when:
- Under time pressure (emergencies make guessing tempting)
- "Just one quick fix" seems obvious
- You've already tried multiple fixes
- Previous fix didn't work
- You don't fully understand the issue

**Don't skip when:**
- Issue seems simple (simple bugs have root causes too)
- You're in a hurry (rushing guarantees rework)
- Manager wants it fixed NOW (systematic is faster than thrashing)

---

## The Four Phases

You MUST complete each phase before proceeding to the next.

### Phase 1: Root Cause Investigation

BEFORE attempting ANY fix:

**Read Error Messages Carefully**
- Don't skip past errors or warnings — they often contain the exact solution
- Read stack traces completely
- Note line numbers, file paths, error codes

**Reproduce Consistently**
- Can you trigger it reliably?
- What are the exact steps?
- If not reproducible → gather more data, don't guess

**Check Recent Changes**
- What changed that could cause this?
- Git diff, recent commits, new dependencies, config changes, environmental differences

**Gather Evidence in Multi-Component Systems**

When the system has multiple components (CI → build → signing, API → service → database), add diagnostic instrumentation at each component boundary before proposing fixes:

```bash
# For EACH component boundary:
# - Log what data enters component
# - Log what data exits component
# - Verify environment/config propagation
# - Check state at each layer
```

Run once to gather evidence showing WHERE it breaks, then analyze to identify the failing component, then investigate that specific component.

**Trace Data Flow**

When the error is deep in the call stack, trace backward:
- Where does the bad value originate?
- What called this with the bad value?
- Keep tracing up until you find the source
- Fix at source, not at symptom

---

### Phase 2: Pattern Analysis

**Find Working Examples**
- Locate similar working code in the same codebase
- What works that's similar to what's broken?

**Compare Against References**
- If implementing a pattern, read the reference implementation completely — don't skim
- Understand the pattern fully before applying

**Identify Differences**
- What's different between working and broken?
- List every difference, however small — don't assume "that can't matter"

**Understand Dependencies**
- What other components does this need?
- What settings, config, environment?
- What assumptions does it make?

---

### Phase 3: Hypothesis and Testing

**Form Single Hypothesis**
- State clearly: "I think X is the root cause because Y"
- Write it down. Be specific, not vague.

**Test Minimally**
- Make the SMALLEST possible change to test the hypothesis
- One variable at a time — don't fix multiple things at once

**Verify Before Continuing**
- Did it work? Yes → Phase 4
- Didn't work? Form a NEW hypothesis. DON'T add more fixes on top.

**When You Don't Know**
- Say "I don't understand X"
- Don't pretend to know — research more before forming a hypothesis

**Fix Confidence Is Capped by Reproduction**

Source analysis alone — reading code, tracing the call path, identifying the missing guard — can justify a root cause hypothesis. It cannot justify a fix.

| Reproduction status | Max fix confidence |
|---|---|
| Reproduced locally, fix verified | High |
| Not reproduced (timing, env, seeds) | Medium — fix is a hypothesis |
| Cannot reproduce at all | Low — root cause may be wrong |

If you propose a fix without a reproducer, state this explicitly: "Fix proposed from source analysis; confidence is Medium until a reproduction verifies the change eliminates the assertion." Do not report High confidence on a fix you have not tested.

---

### Phase 4: Implementation

**Create Failing Test Case**
- Simplest possible reproduction
- MUST have before fixing
- Write a test (or a one-off repro script) that fails for the right reason before attempting any fix

**Implement Single Fix**
- Address the root cause identified
- ONE change at a time
- No "while I'm here" improvements, no bundled refactoring

**Verify Fix**ss
- Test passes now? No other tests broken? Issue actually resolved?

**If Fix Doesn't Work — STOP**
- Count: How many fixes have you tried?
- If < 3: Return to Phase 1, re-analyze with new information
- If ≥ 3: STOP and question the architecture (see below)
- DON'T attempt Fix #4 without architectural discussion

**If 3+ Fixes Failed: Give Up**

Signs of an architectural problem:
- Each fix reveals new shared state/coupling/problem in a different place
- Fixes require "massive refactoring" to implement
- Each fix creates new symptoms elsewhere

STOP. Record what was tried and why each failed. Mark the investigation as "Insufficient data — architectural issue suspected" and do not attempt further fixes.

---

## Confidence Ladder — Be Exhaustive

Do not stop at source analysis when more evidence is reachable. Each rung raises
confidence; skip a rung only when it is genuinely impossible (environment unavailable,
can't build, etc.) — and say so explicitly.

| Rung | Action | Confidence gained |
|---|---|---|
| 1 | Read the failing assertion and call stack in source | Root cause hypothesis |
| 2 | Reproduce the failure locally (meet minimum iteration count) | Root cause confirmed |
| 3 | Implement the minimal fix | Fix hypothesis |
| 4 | Rebuild and re-run the same repro to verify the failure is gone | Fix confirmed — High |
| 5 | Run a broader regression pass (related tests / longer timer) | No regressions introduced |

**The iron rule:** Do not stop at Rung 1 or 2 and call it done. If you can reproduce,
fix. If you can fix, verify. If you can verify, run a regression pass. Work down the
ladder until a rung is genuinely blocked, then state why you stopped.

When you stop early, say:
> "Stopped at Rung N — `<specific blocker>`. Confidence: Medium."

Never claim High confidence without reaching Rung 4.

---

## Red Flags — STOP and Follow Process

If you catch yourself thinking any of these, return to Phase 1:

- "Quick fix for now, investigate later"
- "Just try changing X and see if it works"
- "Add multiple changes, run tests"
- "Skip the test, I'll manually verify"
- "It's probably X, let me fix that"
- "I don't fully understand but this might work"
- "Here are the main problems: [lists fixes without investigation]"
- Proposing solutions before tracing data flow
- "One more fix attempt" (when already tried 2+)
- Each fix reveals a new problem in a different place

---

## Common Rationalizations

| Excuse | Reality |
|---|---|
| "Issue is simple, don't need process" | Simple issues have root causes too. Process is fast for simple bugs. |
| "Emergency, no time for process" | Systematic debugging is FASTER than guess-and-check thrashing. |
| "Just try this first, then investigate" | First fix sets the pattern. Do it right from the start. |
| "I'll write test after confirming fix works" | Untested fixes don't stick. Test first proves it. |
| "Multiple fixes at once saves time" | Can't isolate what worked. Causes new bugs. |
| "Reference too long, I'll adapt the pattern" | Partial understanding guarantees bugs. Read it completely. |
| "I see the problem, let me fix it" | Seeing symptoms ≠ understanding root cause. |
| "One more fix attempt" (after 2+ failures) | 3+ failures = architectural problem. Question pattern, don't fix again. |

---

## Quick Reference

| Phase | Key Activities | Success Criteria |
|---|---|---|
| 1. Root Cause | Read errors, reproduce, check changes, gather evidence | Understand WHAT and WHY |
| 2. Pattern | Find working examples, compare | Identify differences |
| 3. Hypothesis | Form theory, test minimally | Confirmed or new hypothesis |
| 4. Implementation | Create test, fix, verify | Bug resolved, tests pass |

---

## When Process Reveals "No Root Cause"

If systematic investigation reveals the issue is truly environmental, timing-dependent, or external:
- You've completed the process — document what you investigated
- Implement appropriate handling (retry, timeout, error message)
- Add monitoring/logging for future investigation

But: **95% of "no root cause" cases are incomplete investigation.**
