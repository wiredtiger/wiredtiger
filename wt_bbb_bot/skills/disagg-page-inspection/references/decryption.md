# Decryption Workflows

## Table of Contents
- [Overview](#overview)
- [CLI Reference](#cli-reference)
- [Workflow A: Simple Page (No Deltas)](#workflow-a-simple-page-no-deltas)
- [Workflow B: Page with Deltas](#workflow-b-page-with-deltas)
- [Analyzing Decrypted Output](#analyzing-decrypted-output)
- [Troubleshooting](#troubleshooting)

---

## Overview

Pages stored in SLS are encrypted. The `page.contents` bytes from the page service contain an Encrypted Page Header followed by encrypted WiredTiger page data. Decryption is two-stage:

1. Encrypted Page Header → extract DEK (Data Encryption Key), which is encrypted with the KEK.
2. Decrypt DEK using KEK → decrypt page contents using DEK.

**Binary**: `bazel-bin/src/mongo/db/modules/atlas/src/disagg_storage/encryption/pagedecryptor`

**Build**:
```bash
bazel build --config=dbg --//bazel/config:build_atlas \
  //src/mongo/db/modules/atlas/src/disagg_storage/encryption:pagedecryptor
```

---

## CLI Reference

```
Usage: pagedecryptor [options]

Required (all modes):
  --outputPath <path>     Output binary file (wt checkpoint format)
  --keyFile <path>        Path to KEK key file (base64-encoded, mode 600)
  --lsn <int>             LSN of the page
  --tableId <int>         Table ID
  --pageId <int>          Page ID

Input (choose one):
  --jsonPage <string>     Full gRPC JSON response string (simple pages only)
  --inputPath <path>      Path to base64-encoded encrypted page file

Optional:
  --backlinkLsn <int>     Backlink LSN (default: 0)
  --baseLsn <int>         Base LSN for delta pages (default: 0)
  --checkpointId <int>    Checkpoint ID (default: 0)
  --isDelta               Flag: page is a delta (not a full image)
```

### Choosing the Input Mode

| Mode | When to Use |
|------|-------------|
| `--jsonPage "$(cat file.json)"` | Simple pages: gRPC response has only `contents`, no `deltas` |
| `--inputPath file.b64` | Pages with deltas, or any page after extracting base64 to file |

**Rule**: If the gRPC response contains a `deltas` array, always use `--inputPath`. The `--jsonPage` mode cannot parse complex JSON with delta arrays.

---

## Workflow A: Simple Page (No Deltas)

```bash
# 1. Save gRPC response to a JSON file (copy full grpcurl output into page.json)

# 2. Decrypt directly from JSON
pagedecryptor \
  --jsonPage "$(cat page.json)" \
  --outputPath ./decrypted_page_<LOG>_<TABLE>_<PAGE>.bin \
  --keyFile /data/db/job0/mongorunner/decrypt_key \
  --lsn <LSN> \
  --tableId <TABLE> \
  --pageId <PAGE>
```

---

## Workflow B: Page with Deltas

When a page response contains a full image (`contents`) plus `deltas`, decrypt each piece separately using `--inputPath` mode.

### Step 1: Extract base64 pieces from the gRPC JSON

For each piece, extract the base64 string from the JSON and save to a `.b64` file:
- `contents` → `fullimage.b64`
- Each element in `deltas` → `delta_0.b64`, `delta_1.b64`, etc.

Also note the LSN metadata from the response: `fullImageLsn`, `fullImageBacklinkLsn`, `lsns[]`, `backlinkLsns[]`, `baseLsn`.

### Step 2: Decrypt the full image (no `--isDelta` flag)

```bash
pagedecryptor \
  --inputPath fullimage.b64 \
  --outputPath decrypted_fullimage.bin \
  --keyFile /data/db/job0/mongorunner/decrypt_key \
  --lsn <fullImageLsn> \
  --backlinkLsn <fullImageBacklinkLsn> \
  --tableId <TABLE_ID> \
  --pageId <PAGE_ID>
```

### Step 3: Decrypt each delta (`--isDelta` required)

```bash
pagedecryptor \
  --inputPath delta_0.b64 \
  --outputPath decrypted_delta_0.bin \
  --keyFile /data/db/job0/mongorunner/decrypt_key \
  --lsn <lsns[0]> \
  --backlinkLsn <backlinkLsns[0]> \
  --baseLsn <baseLsn> \
  --tableId <TABLE_ID> \
  --pageId <PAGE_ID> \
  --isDelta
```

Repeat for each delta index N.

---

## Analyzing Decrypted Output

```bash
# Hex dump
hexdump -C decrypted_page.bin

# WiredTiger decoder (preferred)
python3 /home/ubuntu/wiredtiger/tools/wt_binary_decode.py \
  --disagg --verbose --bson decrypted_page.bin
```

> Pages from `log_id >= 2` may report "bad magic number" from `wt_binary_decode.py` — use `hexdump` for those.

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Failed getting DEK for decryption` | Invalid or missing key file | Verify key file path and `chmod 600` |
| `Unrecognized AES key size` | Key not properly base64-encoded 256-bit key | Use the actual KEK from the test environment |
| `--jsonPage` silently fails / crash | JSON contains `deltas` array | Switch to `--inputPath` with extracted b64 files |
| `bad magic number` in decoder | Disagg page format (not standard WT) | Use `hexdump -C` instead |
