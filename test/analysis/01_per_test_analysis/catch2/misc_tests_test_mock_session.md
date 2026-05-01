# test_mock_session — mock_session utility class tests

**File:** `test/catch2/misc_tests/test_mock_session.cpp`
**Storage mode:** General
**Components under test:** `mock_session` utility class, error handler callbacks
**Test type:** Unit

## TEST_CASE: "mock_session utility class" [mock_session]
- **What it tests:**
  - `add_callback_message` and `get_last_message` store and retrieve error/message strings correctly.
  - `handle_error` invokes the registered error callback and stores the message.
  - `handle_message` invokes the registered message callback and stores the message.
  - `handle_close`, `handle_general`, and `handle_progress` are verified to be NULL (not implemented in the mock).
- **Components:** `mock_session`, `mock_session::build_test_mock_session`
- **Notes:** This test validates the test infrastructure itself, ensuring the mock session can be used reliably in other unit tests.
