# test_layered64 — Checkpoint metadata checksum validation

**File:** `test/suite/test_layered64.py`
**Storage mode:** Disagg/Layered
**Components under test:** Checkpoint metadata integrity, checksum verification, follower checkpoint pickup

## Test Cases

### `test_layered64.test_layered64`
- **What it tests:** Three-phase test of checkpoint metadata checksum handling:
  1. After a leader checkpoint, confirms the metadata string contains a `metadata_checksum=` field.
  2. After a fresh restart (no local files), verifies the follower can still pick up the checkpoint when the checksum field is stripped from the metadata string (backward-compatible path), and that a warning is emitted (`Missing metadata_checksum from metadata:`). FIXME-WT-16000 tracks making the field mandatory.
  3. After a second restart, corrupts the checksum by XOR-ing it with 0xFF and verifies that `reconfigure(checkpoint_meta=...)` raises `WiredTigerError` with the message "Checkpoint metadata corruption detected".
- **Components:** `src/conn/conn_disagg.c` (metadata checksum), page log extension, `src/conn/conn_ckpt.c`
- **Notes:** Uses regex to extract and manipulate the hex checksum from the metadata string. Disagg-only. Tests both the missing-checksum tolerance (FIXME) and the corruption detection path.
