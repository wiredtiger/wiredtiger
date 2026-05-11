# Table Navigation

## Table of Contents
- [Turtle Page](#turtle-page)
- [Shared Metadata Table](#shared-metadata-table)
- [MongoDB Catalog](#mongodb-catalog)
- [MongoDB Oplog](#mongodb-oplog)
- [MongoDB Index](#mongodb-index)
- [WiredTiger History Store](#wiredtiger-history-store)

---

## Turtle Page

The turtle page is the entry point for all navigation. It stores the root page address of the shared metadata table (`WiredTigerShared.wt_stable`). Always at `table_id=1, page_id=1`.

### Fetch the turtle page

Find the latest LSN:
```bash
grpcurl -plaintext -d '{"log_id": 1, "table_id": 1, "page_id": 1}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.GetPageHistory
```

Fetch:
```bash
grpcurl -plaintext -d '{
  "log_id": 1, "table_id": 1, "page_id": 1, "lsn": <LSN>
}' $PAGE_SERVER pageservice.v1.PageService.GetPageAtLSN
```

### Interpret

After decryption, the turtle page is **plaintext** (not a WiredTiger page). It contains a checkpoint string:

```
WiredTigerCheckpoint.116=(addr="00c09880e869252cb0ffffdfc5e869252cb0ffffdfc5c00a5d4a4c25",
  order=116,time=1764043953,size=216,newest_start_durable_ts=0,oldest_start_ts=0,
  newest_stop_durable_ts=-1,newest_stop_ts=-1,newest_stop_txn=-2,
  prepare=0,write_gen=233,run_write_gen=1,next_page_id=217)
```

### Extract root page address

Decode the `addr` field:
```bash
python3 /home/ubuntu/wiredtiger/tools/wt_disagg_addr_decode.py \
  00c09880e869252cb0ffffdfc5e869252cb0ffffdfc5c00a5d4a4c25
```

This gives `page_id`, `lsn`, `base_lsn` for the root page of the shared metadata table.

---

## Shared Metadata Table (WiredTigerShared.wt_stable)

Contains configuration and checkpoint info for all WiredTiger tables. Maps file names → config strings with `table_id` and checkpoint address cookies.

Currently `table_id=9` locally (not guaranteed; see WT-14536).

### Navigate to metadata

1. Start from turtle page → get root page address.
2. Fetch root page of metadata table.
3. Decode. If internal page, follow child addresses to traverse B-tree.

Or fetch the entire table:
```bash
python3 /home/ubuntu/wiredtiger/tools/disagg_tree_fetch/disagg_fetch_full_tree.py \
  --log-id 1 --table-id 9 \
  --root-page-id <ROOT_PAGE_ID> --root-lsn <ROOT_LSN> \
  --page-server $PAGE_SERVER \
  --key-file /data/db/job0/mongorunner/decrypt_key \
  --output-dir ./metadata_output
```

### Read metadata entries

Keys are file names, values are config strings:
```
8: short key 28 bytes
  "file:_mdb_catalog.wt_stable"
9: val 1472 bytes
  ...id=49...checkpoint=(WiredTigerCheckpoint.1=(addr="00c02580e869645dbb..."))
```

### Find a table_id

Look up the file name in the metadata table. The `id` field in the config string = `table_id` for SLS. The `checkpoint` field has the `addr` cookie → decode for root page address.

---

## MongoDB Catalog (_mdb_catalog.wt_stable)

Maps MongoDB namespaces to WiredTiger filenames. Entries are BSON documents.

### Find the catalog

From metadata, look up `file:_mdb_catalog.wt_stable` to get its `table_id` and checkpoint address.

Example: `id=49` → catalog's `table_id` is 49.

### Read catalog entries

Decode with `--bson`:
```bash
python3 /home/ubuntu/wiredtiger/tools/wt_binary_decode.py --disagg --verbose --bson catalog_page.bin
```

Each entry maps namespace → ident:
```python
{ 'ident': 'collection-03e85044-cbd1-4825-8e3d-f9a89e44972b',
  'idxIdent': { '_id_': 'index-YYYY' },
  'ns': 'local.oplog.rs' }
```

---

## MongoDB Oplog

### Step 1: Find ident from catalog

Locate the entry where `ns` is `local.oplog.rs`:
```python
{ 'ident': 'collection-03e85044-cbd1-4825-8e3d-f9a89e44972b',
  'ns': 'local.oplog.rs' }
```

### Step 2: Find table_id from metadata

Look up `file:collection-03e85044-cbd1-4825-8e3d-f9a89e44972b.wt_stable` in the shared metadata table → extract `id` (e.g. `id=65`).

### Step 3: Fetch and decode

```bash
python3 /home/ubuntu/wiredtiger/tools/disagg_tree_fetch/disagg_fetch_full_tree.py \
  --log-id 1 --table-id 65 \
  --root-page-id <ROOT_PAGE_ID> --root-lsn <ROOT_LSN> \
  --page-server $PAGE_SERVER \
  --key-file /data/db/job0/mongorunner/decrypt_key \
  --output-dir ./oplog_output --bson
```

---

## MongoDB Index

### Step 1: Find index ident from catalog

The catalog entry has `idxIdent` mapping index names to idents:
```python
{ 'ident': 'collection-XXXX',
  'idxIdent': { '_id_': 'index-YYYY' },
  'ns': 'test.mycollection' }
```

### Step 2: Find table_id from metadata

Look up `file:index-YYYY.wt_stable` in the shared metadata table.

### Step 3: Fetch and decode

Same approach as collections. Note: index keys use special packed format — decoding not yet supported (tracked for future work).

---

## WiredTiger History Store

Same process as any table:

1. Look up `file:WiredTigerSharedHS.wt_stable` in metadata → get `table_id` and checkpoint address.
2. Decode the checkpoint address cookie → root page location.
3. Fetch and decode the root page, traverse as needed.
