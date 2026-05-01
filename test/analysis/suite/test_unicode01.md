# test_unicode01 — UTF-8 config string accepted by WT_SESSION::create

**File:** `test/suite/test_unicode01.py`
**Storage mode:** General
**Components under test:** `session.create`, UTF-8 config parsing, `app_metadata`

## Test Cases

### `test_unicode01.test_unicode`
- **What it tests:** Calls `session.create` with a metadata string containing valid Unicode characters (U+222B integral sign, U+67D2 CJK character, U+D4DB Korean syllable) in the `app_metadata` field, verifying that Python's UTF-8 encoding of Unicode strings is correctly accepted by the WiredTiger configuration parser without error.
- **Components:** `config.c`, `schema.c`
- **Notes:** No parameterization. Tests that the configuration parser handles multi-byte UTF-8 sequences in metadata values.
