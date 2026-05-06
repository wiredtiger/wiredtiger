# scope — Cursor key/value memory scope validation

**Path:** `test/csuite/scope/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Cursor API memory ownership, `cursor->get_key`, `cursor->get_value`, `cursor->insert`, `cursor->update`, `cursor->remove`, `cursor->search`, `cursor->search_near`, `cursor->modify`, `cursor->reserve`

## What This Test Does
This test verifies that after each cursor operation, the cursor no longer references application-provided memory. It sets up key and value buffers, performs a cursor operation, overwrites those buffers with sentinel bytes, then calls `get_key`/`get_value` to confirm the cursor's retained references do not point into the now-overwritten application buffers. It also validates that operations which do not position the cursor (plain insert and remove-by-key) correctly return an error when `get_key`/`get_value` is called afterward. Tests run across all combinations of file/table URIs and row/column key formats with string/raw value formats (8 combinations total).

## Test Scenarios / Cases

### Scenario: Insert (key scope — no cursor position)
- **What it tests:** That after `cursor->insert()`, calling `cursor->get_key()` returns an error (insert does not leave the cursor positioned).
- **Components:** Cursor insert, memory scope.
- **Notes:** Triggers two expected error messages (key and value not available); requires rollback.

### Scenario: Insert (value scope — no cursor position)
- **What it tests:** That after `cursor->insert()`, calling `cursor->get_value()` returns an error.
- **Components:** Cursor insert, memory scope.
- **Notes:** Same expected-error behavior as key scope.

### Scenario: Search (positioned)
- **What it tests:** That after `cursor->search()`, both `get_key` and `get_value` return values copied into library-owned memory (not pointing into the application buffer).
- **Components:** Cursor search, memory scope.
- **Notes:** Application key/value buffers are overwritten and the cursor value is compared.

### Scenario: Search-near (positioned)
- **What it tests:** Same as search, but via `cursor->search_near()`.
- **Components:** `search_near`, memory scope.

### Scenario: Reserve (positioned)
- **What it tests:** That after `cursor->reserve()`, both key and value are available and not referencing application memory.
- **Components:** `cursor->reserve`, memory scope.

### Scenario: Modify (positioned)
- **What it tests:** That after `cursor->modify()`, both key and value are available and library-owned.
- **Components:** `cursor->modify`, memory scope.

### Scenario: Update (positioned)
- **What it tests:** That after `cursor->update()`, both key and value are available and library-owned.
- **Components:** `cursor->update`, memory scope.

### Scenario: Remove by key (no cursor position)
- **What it tests:** That after `cursor->remove()` without a prior search, `get_key` and `get_value` return errors.
- **Components:** `cursor->remove`, memory scope.

### Scenario: Remove by cursor position (key available, value not)
- **What it tests:** That after a search-then-remove, `get_key` returns the key (cursor retains key position) but `get_value` returns an error.
- **Components:** Positioned cursor remove, memory scope.

## LazyFS Variant
None.
