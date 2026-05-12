# Reproduction

Subagent path for local reproduction. Called from @paths/investigate.md Step 8.

For fix proposal and verification after a root cause is confirmed, see @paths/build.md.

## Inputs (from investigate.md)

- Ticket key and one-line summary
- Test name / command (from Step 3)
- Build variant (from Step 3: ASan / debug / release)
- Suspect commit / location (from Steps 6–7, for context only)
- Min iterations (from Step 5: 10 / 20 / 30)

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Reproduce a WiredTiger BF locally. No fix proposals. No code changes.

Inputs:
- Ticket: WT-XXXXX — <summary>
- Test name / command: <from investigate.md Step 3>
- Build variant: <ASan / debug / release>
- Suspect commit / location: <from Steps 6–7, context only>
- Min iterations: <10 / 20 / 30>

Before writing a new Python test, search test/suite/ for analogous tests that
exercise the same scenario (e.g. prepared transactions, disagg role switching, RTS).
Copy their setup pattern exactly rather than deriving from first principles.

Steps (follow @paths/build.md — "Reproducing a BF Failure" section):
1. Build the matching variant if the build dir does not exist.
2. Run the exact failing test at the minimum iteration count.

Return only:
- mode: investigation
- command: <exact command run>
- build variant: <value>
- iterations run: N
- result: reproduced | not reproduced | inconclusive
- failure rate: X/N
- first failure log: <path or "n/a">
- first error line: <exact quoted string or "n/a">
"""
)
```

## Returns to investigate.md

- **Result:** `reproduced | not reproduced | inconclusive | skipped — <reason>`
- **Command:** `<exact command run>`
- **Build variant:** `<value>`
- **Iterations run:** N
- **Failure rate:** X/N
- **First failure log:** `<path, or "n/a">`
- **First error line:** `<exact quoted string, or "n/a">`
