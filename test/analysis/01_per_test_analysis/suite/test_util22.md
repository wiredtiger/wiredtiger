# test_util22 — wt CLI: help option and invalid option/command handling

**File:** `test/suite/test_util22.py`
**Storage mode:** General
**Components under test:** `wt` CLI argument parsing, help/usage output for all subcommands

## Test Cases

### `test_util22.test_help_option`
- **What it tests:** Runs `wt -?` and verifies output contains `'global_options:'`; then for each subcommand in [alter, backup, compact, create, downgrade, drop, dump, list, load, loadtext, printlog, read, salvage, stat, truncate, verify, write], runs `wt <cmd> -?` and verifies output contains `'options:'`.
- **Components:** `util.c` (main argument parser)
- **Notes:** Excludes `copyright` which does not process options. Tests that every subcommand implements `-?` help.

### `test_util22.test_no_argument`
- **What it tests:** Runs `wt -h` (option that requires an argument) with no argument; verifies error contains `'wt: option requires an argument -- h'` and `'global_options:'`.
- **Components:** `util.c`
- **Notes:** Tests argument-required option error message format.

### `test_util22.test_unsupported_command`
- **What it tests:** Runs `wt unsupported`; verifies error output contains `'global_options:'` (displays usage on unknown command).
- **Components:** `util.c`
- **Notes:** Tests that unknown subcommands print global usage.

### `test_util22.test_unsupported_option`
- **What it tests:** Runs `wt -^` (invalid global option); verifies error contains `'wt: illegal option -- ^'` and `'global_options:'`; then for each subcommand runs `wt <cmd> -^` and verifies `'illegal option -- ^'` and `'options:'`.
- **Components:** `util.c`
- **Notes:** Tests invalid option error handling at both global and subcommand level.
