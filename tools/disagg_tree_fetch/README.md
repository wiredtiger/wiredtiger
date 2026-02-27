# disagg_tree_fetch — Disaggregated Page Tree Fetcher

Fetch, decrypt, decode, and traverse a complete disaggregated storage page tree
starting from a root page.

## Overview

Given a root page identified by `(log_id, table_id, page_id, lsn)`, this tool:

1. **Fetches** each page via the `PageService.GetPageAtLSN` gRPC API (native
   Python `grpcio`).
2. **Decrypts** each page via the `pagedecryptor` command-line tool.
3. **Decodes** each page using the `py_common` WiredTiger binary decoding
   library directly (no subprocess).
4. **Extracts** child page references from internal pages.
5. **Repeats** until the full reachable tree has been visited.

## Prerequisites

**[uv](https://docs.astral.sh/uv/)** is used for dependency management.

```bash
# Install uv (if not already present)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

The `pagedecryptor` binary must be available (on `PATH` or via `--decryptor-path`):

```bash
# Build from the mongo repo:
bazel build //src/mongo/db/modules/atlas/src/disagg_storage/encryption:pagedecryptor
```

## Setup

### 1. Install Python dependencies

```bash
cd tools/disagg_tree_fetch
uv sync
```

This creates a `.venv` from the committed `uv.lock`, ensuring a reproducible environment.
The `uv.lock` file should be committed to the repository.

### 2. Generate gRPC stubs

The Python gRPC stubs are generated from the page service proto files in the mongo
repo and are **not** committed (they are in `.gitignore`). Generate them once:

```bash
./generate_proto.sh [PROTO_DIR]
```

`PROTO_DIR` defaults to `~/mongo/src/mongo/db/modules/atlas/src/disagg_storage/sls-proto/dist/storage/etc/protos`.

The script runs `uv sync` automatically, so no separate activation is needed.

## Usage

```bash
cd tools/disagg_tree_fetch
uv run disagg_fetch_full_tree.py \
    --log-id 1 \
    --table-id 5 \
    --root-page-id 42 \
    --root-lsn 1000 \
    --page-server 172.17.0.1:20044 \
    --key-file /data/db/job0/mongorunner/decrypt_key
```

### Key options

| Flag | Description | Default |
|------|-------------|---------|
| `--log-id` | SLS log ID (shard) | *required* |
| `--table-id` | WiredTiger table ID | *required* |
| `--root-page-id` | Root page to start traversal from | *required* |
| `--root-lsn` | LSN of the root page | *required* |
| `--page-server` | gRPC address of the page server | `172.17.0.1:20044` |
| `--decryptor-path` | Path to `pagedecryptor` binary | `pagedecryptor` |
| `--key-file` | Encryption key file | `/data/db/job0/mongorunner/decrypt_key` |
| `--verbose` / `--no-verbose` | Print cell data (not just headers) | `--verbose` |
| `--bson` | Decode cell values as BSON | off |
| `--output-dir` | Output directory | `/tmp/disagg_tree_<table>_<page>_<lsn>` |
| `--max-pages` | Safety limit (0 = unlimited) | `0` |
| `--debug` | Enable debug logging | off |

> **Note:** Checksum validation requires the optional `crc32c` package
> (`uv add crc32c`). Without it, checksums are skipped with a warning.

## Output

```
<output-dir>/
├── pages/          # Raw page JSON from the gRPC response
├── decrypted/      # Decrypted binary page data
├── decoded/        # Human-readable decoded page text
└── manifest.json   # Summary of all visited pages and their relationships
```

The `manifest.json` contains the full tree structure including child references,
page types, write generations, and paths to all artifacts.
