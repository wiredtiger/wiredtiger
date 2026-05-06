# test_verbose03 — JSON event handler output: schema validation for verbose messages and error messages

**File:** `test/suite/test_verbose03.py`
**Storage mode:** General
**Components under test:** `json_output=[message]`, `json_output=[error]`, JSON verbose message schema

## Test Cases

### `test_verbose03.test_verbose_json_message`
- **What it tests:** Opens a connection with `json_output=[message],verbose=[api]`; performs table create and cursor insert to generate `WT_VERB_API` messages; opens again with `verbose=[api,version]`; for every line on stdout, parses as JSON and validates: (1) all fields are in the expected schema (`category`, `category_id`, `log_id`, `msg`, `thread`, `ts_sec`, `ts_usec`, `verbose_level`, `verbose_level_id`, plus optional `error_str/code`, `session_*`); (2) all always-expected fields are present with correct types; (3) the `category` field is `WT_VERB_API` or `WT_VERB_VERSION` with matching `category_id`.
- **Components:** `verbose.c`, `json.c`, `api.c`
- **Notes:** No parameterization. Uses `expect_event_handler_json` context manager. Expected schema documented in `test_verbose_base.expected_json_schema`.

### `test_verbose03.test_verbose_json_err_message`
- **What it tests:** Opens with `json_output=[error]`; triggers a `WiredTigerError` by calling `session.begin_transaction('read_timestamp=-1')`; captures stderr output; parses each line as JSON and validates the schema; verifies the `category` is `WT_VERB_DEFAULT` with the correct `category_id`.
- **Components:** `verbose.c`, `json.c`, `txn_timestamp.c`
- **Notes:** Uses stderr (stdErr=True). Tests that error messages sent via the error handler also produce valid JSON when `json_output=[error]` is configured.
