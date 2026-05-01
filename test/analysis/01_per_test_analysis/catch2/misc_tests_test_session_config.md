# test_session_config — Session config integer parsing tests (disabled)

**File:** `test/catch2/misc_tests/test_session_config.cpp`
**Storage mode:** General
**Components under test:** `__ut_session_config_int`
**Test type:** Unit

## Status: DISABLED

The entire test file is wrapped in `#ifdef ENABLE_DISABLED_TEST`. No test cases are compiled or executed in normal builds.

If re-enabled, the tests would cover:
- Parsing integer session config flags.
- Parsing `cache_max_wait_ms` as an integer from the session config string.
