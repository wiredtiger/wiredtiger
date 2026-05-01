# test_layered01 — Basic layered table creation and metadata verification

**File:** `test/suite/test_layered01.py`
**Storage mode:** Disagg/Layered
**Components under test:** layered table schema, metadata, ingest btree, stable btree

## Test Cases

### `test_layered01.test_layered01`
- **What it tests:** Creates a layered table via `session.create("layered:<name>", ...)` and verifies that all three expected metadata entries are present: the top-level `layered:` URI, the ingest file (`*.wt_ingest`), and the stable file (`*.wt_stable`). Each entry is checked with `metadata:create` cursor to confirm its presence.
- **Components:** layered table manager (`conn_layered.c`, `conn_layered_ingest.c`), schema layer, WiredTiger metadata store
- **Notes:** Runs only in disagg/palite mode (wrapped by `@disagg_test_class`). Uses `lose_all_my_data=true` (page log discards data on restart). The test is a pure creation smoke-test — it verifies that creating a layered table registers the correct set of sub-file URIs in metadata. Would break if the layered schema handler fails to create the ingest or stable sub-files, or if their metadata keys are misnamed.
