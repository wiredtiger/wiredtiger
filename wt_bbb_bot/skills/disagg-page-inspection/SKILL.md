---
name: disagg-page-inspection
description: End-to-end guide for inspecting WiredTiger pages stored in SLS (Storage Layer Services) for MongoDB's disaggregated storage. Use this skill when navigating the WiredTiger B-tree hierarchy in disagg storage, fetching/decrypting/decoding pages from the page service, looking up table_ids from metadata, finding MongoDB collections or indexes via the catalog, inspecting the turtle page, metadata table, history store, oplog, or any other WiredTiger table in SLS. Also use when recreating persisted test state from Evergreen artifacts, or when debugging data corruption or checkpoint issues in disaggregated storage.
---

# Disaggregated Storage Page Inspection

Inspect WiredTiger pages stored in SLS independently of a running `mongod`. Useful for debugging data corruption, investigating checkpoint issues, and understanding B-tree structure in disagg storage.

## Tool Locations

| Tool | Path |
|------|------|
| pagedecryptor | `bazel-bin/src/mongo/db/modules/atlas/src/disagg_storage/encryption/pagedecryptor` |
| wt_binary_decode.py | `/home/ubuntu/wiredtiger/tools/wt_binary_decode.py` |
| wt_disagg_addr_decode.py | `/home/ubuntu/wiredtiger/tools/wt_disagg_addr_decode.py` |
| disagg_fetch_full_tree.py | `/home/ubuntu/wiredtiger/tools/disagg_tree_fetch/disagg_fetch_full_tree.py` |
| grpcurl | `/home/ubuntu/.ds_toolchain/grpcurl/bin/grpcurl` |
| Test key file | `/data/db/job0/mongorunner/decrypt_key` or `src/mongo/db/modules/atlas/src/disagg_storage/test_keyfile.in` |

### Build pagedecryptor

```bash
bazel build --config=dbg --//bazel/config:build_atlas \
  //src/mongo/db/modules/atlas/src/disagg_storage/encryption:pagedecryptor
```

## Key Tables Reference

| Table | File Name | table_id |
|-------|-----------|----------|
| Turtle page | N/A | 1 (page_id=1) |
| Shared metadata | `WiredTigerShared.wt_stable` | 9 (local, not guaranteed) |
| History Store | `WiredTigerSharedHS.wt_stable` | look up from metadata |
| MongoDB Catalog | `_mdb_catalog.wt_stable` | look up from metadata |
| Collections | `collection-XXXX.wt_stable` | look up from metadata |
| Indexes | `index-XXXX.wt_stable` | look up from metadata |

## Core Workflow: Inspect Any Table

The hierarchy for navigating to any table in SLS:

```
Turtle Page (table_id=1, page_id=1)
  └─ root addr → Shared Metadata (WiredTigerShared.wt_stable)
       └─ file:_mdb_catalog.wt_stable → table_id, checkpoint addr
            └─ MongoDB Catalog (BSON: ns → ident mapping)
                 └─ file:collection-XXXX.wt_stable → table_id, checkpoint addr
                      └─ Collection data (leaf pages with BSON docs)
```

### Step 1: Fetch + decrypt a page

```bash
# Fetch via gRPC
grpcurl -plaintext -d '{
  "log_id": 1, "table_id": <TABLE_ID>,
  "page_id": <PAGE_ID>, "lsn": <LSN>
}' $PAGE_SERVER pageservice.v1.PageService.GetPageAtLSN

# Decrypt (simple page, no deltas)
pagedecryptor \
  --jsonPage "$(cat page.json)" \
  --outputPath ./decrypted.bin \
  --keyFile /data/db/job0/mongorunner/decrypt_key \
  --lsn <LSN> --tableId <TABLE_ID> --pageId <PAGE_ID>
```

For pages with deltas, extract base64 pieces and decrypt separately — see @references/decryption.md.

### Step 2: Decode

```bash
python3 /home/ubuntu/wiredtiger/tools/wt_binary_decode.py \
  --disagg --verbose --bson decrypted.bin
```

- `--disagg`: parse disagg block headers
- `--verbose`: print cell data
- `--bson`: decode BSON values (for catalog/collection pages)

### Step 3: Navigate the tree

Internal pages contain child page addresses as disagg address cookies. Decode them:

```bash
python3 /home/ubuntu/wiredtiger/tools/wt_disagg_addr_decode.py <hex_address>
```

This gives `page_id`, `lsn`, `base_lsn` to fetch the next page.

## Fetch an Entire Table

### Option A: disagg_fetch_full_tree.py (direct page service access)

```bash
python3 /home/ubuntu/wiredtiger/tools/disagg_tree_fetch/disagg_fetch_full_tree.py \
  --log-id 1 \
  --table-id <TABLE_ID> \
  --root-page-id <ROOT_PAGE_ID> \
  --root-lsn <ROOT_LSN> \
  --page-server $PAGE_SERVER \
  --key-file /data/db/job0/mongorunner/decrypt_key \
  --output-dir ./output \
  --bson \
  --decryptor-path $(which pagedecryptor)
```

Outputs: `pages/` (raw JSON), `decrypted/` (binary), `decoded/` (text), `manifest.json`.

Prerequisites: run `./generate_proto.sh` in the `disagg_tree_fetch` directory first.

### Option B: GetTableAtLSN (via Object Read Proxy, requires object-services)

```bash
# Find latest LSN
grpcurl -d '{"log_id": 1, "start_lsn": 0}' -plaintext localhost:32017 \
  objectindexservice.v1.ObjectIndexService/GetIndexedFrontier

# Fetch table
grpcurl -d '{
  "log_id": 1, "table_id": <TABLE_ID>,
  "lsn": <LSN>, "result_format": 2
}' -plaintext localhost:32023 \
  objectreadproxyservice.v1.ObjectReadProxyService/GetTableAtLSN
```

Results go to S3: `s3://sls-minikube/${CLUSTER_SESSION_ID}/temp/orp_get_table_at_lsn_result/`

## Detailed References

| Need | Go to |
|---|---|
| SLS setup, recreating test state, finding page server | @references/setup.md |
| Table navigation (turtle → metadata → catalog → oplog/index) | @references/navigation.md |
| Decoding page types, block headers, address cookies | @references/decoding.md |
| Decryption workflows (simple pages, delta pages, troubleshooting) | @references/decryption.md |
| gRPC API reference (ListPages, GetPageHistory, Verify, stats) | @references/grpc.md |
