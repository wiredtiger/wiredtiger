# test_tailq — TAILQ macro tests

**File:** `test/catch2/misc_tests/test_tailq.cpp`
**Storage mode:** General
**Components under test:** `TAILQ_INSERT_TAIL`, `TAILQ_REMOVE`, `TAILQ_FOREACH`
**Test type:** Unit

## TEST_CASE: "Add and remove items from TAILQ" [tailq]
### SECTION: "correct items in queue"
- **What it tests:** After inserting three items with `TAILQ_INSERT_TAIL`, iterating with `TAILQ_FOREACH` yields the items in insertion order.
- **Components:** `TAILQ_INSERT_TAIL`, `TAILQ_FOREACH`
- **Notes:** Basic linked-list ordering correctness.

### SECTION: "item removal"
- **What it tests:** After removing the middle item with `TAILQ_REMOVE`, the remaining two items are present in the correct order.
- **Components:** `TAILQ_REMOVE`, `TAILQ_FOREACH`
- **Notes:** Verifies that removal does not corrupt the list linkage.

## TEST_CASE: "Attempted removal from empty TAILQ" [tailq]
- **What it tests:** Removing an item that was never inserted into the queue (or removing from an empty queue) does not crash or produce undefined behavior.
- **Components:** `TAILQ_REMOVE`
- **Notes:** Documents safe behavior for the remove-from-empty edge case.
