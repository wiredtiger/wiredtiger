# test_util08 — wt copyright CLI: copyright notice output

**File:** `test/suite/test_util08.py`
**Storage mode:** General
**Components under test:** `wt copyright`

## Test Cases

### `test_util08.test_copyright`
- **What it tests:** Runs `wt copyright`; reads up to 1,000 characters from the output file and verifies the string `'Copyright'` appears in it.
- **Components:** `util.c` (copyright subcommand)
- **Notes:** No parameterization. Minimal smoke test confirming the copyright subcommand produces output containing the expected string.
