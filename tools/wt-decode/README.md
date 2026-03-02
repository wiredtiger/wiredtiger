# wt-decode

WiredTiger Disaggregated Storage Decode Tool.

## Features

- **disagg-browser**: Interactive CLI for browsing disaggregated pages, starting from the turtle page and navigating through the metadata to individual tables and their page trees.
- **fetch-tree**: Non-interactive tool to fetch, decrypt, and decode a full page tree.
- **inspect-page**: Fetch, decrypt, and decode a single page for quick spot-checks.
- **delta-chain**: Visualize the delta chain structure (full image + deltas) for a page at a given LSN.
- **export-tree**: Export a previously-fetched tree manifest to JSON or CSV for analysis.
- **dump-metadata**: Dump the contents of the WiredTiger metadata table (table ID 9).
- **dump-file**: Dump the contents of a specific file (table) by URI.
- **config-show**: Display the active configuration file and its values.

## Installation

Ensure you have `uv` installed.

```bash
uv sync
```

## Configuration

You can create a `.wtd.toml` file to persist default values for common options
so they don't need to be passed on every invocation. The tool searches for config
files in the following order:

1. `.wtd.toml` in the current working directory
2. `~/.config/wtd/config.toml`
3. `~/.wtd.toml`

Example `.wtd.toml`:

```toml
[defaults]
page_server = "172.17.0.1:20044"
key_file = "/data/db/job0/mongorunner/decrypt_key"
log_id = 1
decryptor_path = "/home/ubuntu/mongo/bazel-bin/src/mongo/db/modules/atlas/src/disagg_storage/encryption/pagedecryptor"
```

To see the active config:

```bash
uv run wtd config-show
```

## Usage

### Interactive Browser

```bash
uv run wtd disagg-browser --log-id <log_id>
```

### Fetch Tree

```bash
uv run wtd fetch-tree --log-id <log_id> --table-id <table_id> --root-page-id <page_id> --root-lsn <lsn>
```

### Inspect a Single Page

```bash
uv run wtd inspect-page --table-id <table_id> --page-id <page_id> --lsn <lsn>
```

### Delta Chain Visualization

```bash
# Show delta chain structure for a page
uv run wtd delta-chain --table-id <table_id> --page-id <page_id> --lsn <lsn>

# Also include full page version history
uv run wtd delta-chain --table-id <table_id> --page-id <page_id> --lsn <lsn> --history
```

### Dump File/Metadata Contents

```bash
# Dump metadata table (ID 9) to stdout
uv run wtd dump-metadata

# Dump metadata table to a file
uv run wtd dump-metadata -o /tmp/meta.txt

# Dump metadata table (values only)
uv run wtd dump-metadata --values-only

# Dump a collection (keys and values as strings)
uv run wtd dump-file file:collection-7-4886566060411874404.wt

# Dump a collection to a file
uv run wtd dump-file file:collection-7-4886566060411874404.wt -o /tmp/dump.txt

# Dump a collection (values only)
uv run wtd dump-file file:collection-7-4886566060411874404.wt --values-only

# Dump a collection (as BSON)
uv run wtd dump-file file:collection-7-4886566060411874404.wt --bson
```

### Export Tree Data

```bash
# Export as JSON (pipe to jq, etc.)
uv run wtd export-tree /path/to/manifest.json --format json

# Export as CSV (load in pandas, Excel, etc.)
uv run wtd export-tree /path/to/manifest.json --format csv --output tree.csv
```

## Requirements

- `pagedecryptor` must be in your `PATH` or configured in `.wtd.toml`.
- Encryption key file must be accessible.
- Access to the PageService gRPC server.
