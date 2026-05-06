# simulator — Timestamp management simulator for protocol validation

**Path:** `test/simulator/`
**Language:** C++
**Storage mode:** General (no actual WiredTiger database I/O; pure in-memory simulation)
**Components under test:** transaction timestamp validation logic (oldest, stable, durable, read, commit, prepare timestamps), `set_timestamp`, `query_timestamp`, `begin_transaction`, `commit_transaction`, `prepare_transaction`, `rollback_transaction`, `timestamp_transaction`

## Overview

The simulator is a self-contained C++ reimplementation of WiredTiger's timestamp management and transaction lifecycle rules, without any actual storage or B-tree I/O. It exists in two modes: an interactive CLI (`main.c` / `simulator_interface`) through which a developer manually exercises timestamp and session operations and observes validation responses; and a call-log replay mode (`call_log_manager`) that reads a JSON call log (e.g., captured from a real WiredTiger session) and replays it through the simulator to verify that the observed API call sequence is valid according to the timestamp protocol rules.

## Test Scenarios / Cases

### Scenario: Interactive timestamp and session management
- **What it tests:** A menu-driven CLI allows a user to open/close sessions, call `set_timestamp` (connection-level, with oldest/stable/durable), `query_timestamp`, `begin_transaction`, `commit_transaction`, `prepare_transaction`, `rollback_transaction`, and `timestamp_transaction` / `timestamp_transaction_uint`. Each operation is validated by the `timestamp_manager` singleton against the current connection and session state.
- **Components:** `connection_simulator` (singleton: oldest_ts, stable_ts, durable_ts), `session_simulator` (per-session read/commit/prepare timestamps, txn state), `timestamp_manager` (all validation rules)
- **Notes:** The interactive interface is primarily a development/debugging tool. It provides coloured output (RED/GREEN/WHITE ANSI codes) and prints descriptive error messages for invalid operations.

### Scenario: Oldest/stable timestamp validation
- **What it tests:** `timestamp_manager::validate_oldest_and_stable_timestamp` enforces that oldest ≤ stable, that neither goes backward once set, and that the new stable is ≥ the current durable. Sets via `connection_simulator::set_timestamp`.
- **Components:** Oldest and stable timestamp ordering rules, `has_oldest_ts`, `has_stable_ts`
- **Notes:** Hex string parsing (`hex_to_decimal`, `validate_hex_value`) validates that timestamp strings are well-formed hex of ≤16 chars.

### Scenario: Read timestamp validation
- **What it tests:** `validate_read_timestamp` checks that a session's read timestamp is ≤ oldest_ts when set during `begin_transaction`. Rejects reads at timestamps that have already been cleaned up.
- **Components:** Read timestamp, oldest timestamp interaction
- **Notes:** Part of the `begin_transaction` configuration parsing.

### Scenario: Commit timestamp validation
- **What it tests:** `validate_commit_timestamp` checks that the commit timestamp is ≥ the session's prepare timestamp (if prepared), ≥ the connection's oldest timestamp, and consistent with other per-session rules (e.g., cannot be set before `begin_transaction`).
- **Components:** Commit timestamp, prepare timestamp, oldest timestamp ordering
- **Notes:** Used by both `commit_transaction` and `timestamp_transaction`.

### Scenario: Durable timestamp validation
- **What it tests:** `validate_session_durable_timestamp` and `validate_conn_durable_timestamp` check that the session-level durable timestamp is ≥ the commit timestamp, and that the connection-level global durable advances monotonically.
- **Components:** Durable timestamp ordering at session and connection level
- **Notes:** The connection-level durable is updated by `set_global_durable_ts` after each committed transaction.

### Scenario: Call-log replay mode
- **What it tests:** `call_log_manager` reads a JSON file containing a sequence of API calls (open_session, begin_transaction, set_timestamp, commit_transaction, etc.) and replays them through the simulator, asserting that each call either succeeds or fails as recorded in the log. This mode is intended to validate that a real WiredTiger API call sequence adheres to the timestamp protocol.
- **Components:** JSON call log parsing (nlohmann/json), all session and connection APIs via `api_method` enum, session identity mapping by string ID
- **Notes:** Supported API methods: `begin_transaction`, `close_session`, `commit_transaction`, `open_session`, `prepare_transaction`, `query_timestamp`, `rollback_transaction`, `set_timestamp`, `timestamp_transaction`, `timestamp_transaction_uint`.

## Coverage Notes

The simulator uniquely validates WiredTiger's timestamp protocol rules in isolation, without the complexity of actual storage, eviction, or concurrency. It is the only component that makes the timestamp validation rules explicit and machine-checkable in a standalone form. The call-log replay mode is particularly valuable for post-mortem analysis of timestamp-related failures in real workloads — a captured call sequence can be replayed to pinpoint exactly which API call violated a rule. Gaps: the simulator does not model concurrency (all operations are sequential); it does not model rollback-to-stable, checkpoint timestamps, or the history store; it does not integrate with the Python test suite and has no automated test cases (only interactive or log-driven usage); the interactive mode has no pass/fail criteria for CI.
