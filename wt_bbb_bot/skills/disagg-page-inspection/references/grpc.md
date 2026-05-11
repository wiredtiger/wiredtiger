# Page Server gRPC API Reference

## Table of Contents
- [Connection](#connection)
- [Services](#services)
- [Reading Pages](#reading-pages)
- [Discovering Pages](#discovering-pages)
- [Debug / Test Operations](#debug--test-operations)
- [Restore Operations](#restore-operations)

---

## Connection

- **Default endpoint**: `172.17.0.1:20044` (plaintext, no TLS)
- **grpcurl binary**: `/home/ubuntu/.ds_toolchain/grpcurl/bin/grpcurl`
- **Server reflection**: enabled — no `--proto` flag required
- **Proto files** (if reflection unavailable): `src/mongo/db/modules/atlas/src/disagg_storage/sls-proto/dist/storage/etc/protos/`

### Finding the endpoint

The page server host and port are in `sls_cluster.json` under `/data/db/job0/mongorunner/tests/*/`. Look for the entry with `svcType: page`.

### Base command pattern

```bash
grpcurl -plaintext -d '<JSON>' $PAGE_SERVER <service>/<method>
```

### Discover all services and methods

```bash
grpcurl -plaintext $PAGE_SERVER list
grpcurl -plaintext $PAGE_SERVER list pageservice.v1.PageService
grpcurl -plaintext $PAGE_SERVER describe pageservice.v1.PageService.GetPageAtLSN
```

---

## Services

| Service | Purpose |
|---------|---------|
| `pageservice.v1.PageService` | Production: read/write pages, restore, discard |
| `pageservice.v1.PageServiceTestService` | Debug: stats, history, verify, compact, list pages |
| `pageservice.v1.PageServiceMetricsService` | Metrics scraping |
| `pageservice.v1.PageServerControlService` | Heat management signals |

---

## Reading Pages

### GetPageAtLSN (unary — preferred for scripting)

```bash
grpcurl -plaintext -d '{
  "log_id": 2,
  "table_id": 969,
  "page_id": 103,
  "lsn": 7608437386933960705
}' $PAGE_SERVER pageservice.v1.PageService.GetPageAtLSN
```

Optional fields: `"storage_tier": "STORAGE_TIER_COLD"`, `"timeout_ms": 30000`

### GetPageAtLSNStreaming (streaming variant)

```bash
grpcurl -plaintext -d '{
  "log_id": 2,
  "table_id": 969,
  "page_id": 103,
  "lsn": 7608437386933960705
}' $PAGE_SERVER pageservice.v1.PageService.GetPageAtLSNStreaming
```

**Response**: JSON with `page.contents` (base64 full image), optional `page.deltas` (array of base64), LSN arrays, and `pageServersContacted`.

---

## Discovering Pages

### ListPages — enumerate pages for a log/table

```bash
# All pages for a log
grpcurl -plaintext -d '{"log_id": 2}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.ListPages

# Filter by table
grpcurl -plaintext -d '{"log_id": 2, "table_id": 969}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.ListPages

# Filter by table + page with limit
grpcurl -plaintext -d '{"log_id": 2, "table_id": 969, "page_id": 103, "limit": 10}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.ListPages
```

Returns `pageSummaries[]` with `logId`, `tableId`, `pageId`, `lsn`, `contentsSize`, `deltaSizes[]`.

### GetPageHistory — all versions of a specific page

```bash
grpcurl -plaintext -d '{
  "log_id": 2,
  "table_id": 969,
  "page_id": 103
}' $PAGE_SERVER pageservice.v1.PageServiceTestService.GetPageHistory
```

Returns `metadata[]` with `lsn`, `backlinkLsn`, `baseLsn`, `flags` (DELTA/FULL_IMAGE/TOMBSTONE/CHECKPOINT_END), `contentLength`.

### GetStorageStats — page counts and sizes per table

```bash
# All logs
grpcurl -plaintext -d '{}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.GetStorageStats

# Specific log + table
grpcurl -plaintext -d '{"log_id": 2, "table_id": 969}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.GetStorageStats
```

Returns `stats[]` with `pageCount`, `fullCount`, `deltaCount`, `tombstoneCount`, `sizeBytes`, `maxLsn`.

---

## Debug / Test Operations

### GetStats — raw internal stats dump

```bash
grpcurl -plaintext -d '{}' $PAGE_SERVER pageservice.v1.PageServiceTestService.GetStats
```

### GetWaiterList — in-flight GetPageAtLSN waiters

```bash
grpcurl -plaintext -d '{}' $PAGE_SERVER pageservice.v1.PageServiceTestService.GetWaiterList
```

### Verify — integrity check (missing full images / broken delta chains)

```bash
grpcurl -plaintext -d '{}' $PAGE_SERVER pageservice.v1.PageServiceTestService.Verify

# Specific log only
grpcurl -plaintext -d '{"log_id": 2}' $PAGE_SERVER pageservice.v1.PageServiceTestService.Verify
```

Returns `pages`, `fullImages`, `deltas`, `tombstones` counts.

### ForceCompaction

```bash
grpcurl -plaintext -d '{"bypass_heuristics": true}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.ForceCompaction
```

### ForceGarbageCollection

```bash
grpcurl -plaintext -d '{}' $PAGE_SERVER pageservice.v1.PageServiceTestService.ForceGarbageCollection
```

### DropTable — remove a table's pages from the page server

```bash
grpcurl -plaintext -d '{"log_id": 2, "table_id": 969}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.DropTable
```

---

## Restore Operations

### StartRestore

```bash
grpcurl -plaintext -d '{
  "log_id": 2,
  "exclusive_start_lsn": 1000,
  "inclusive_end_lsn": 9999
}' $PAGE_SERVER pageservice.v1.PageService.StartRestore
```

Returns `token` (uint64) for tracking.

### GetRestoreStatus

```bash
grpcurl -plaintext -d '{"token": 12345}' \
  $PAGE_SERVER pageservice.v1.PageService.GetRestoreStatus
```

Returns `progressPercent` (0 = in-progress, 100 = done) and `error` string.

### ListRestoreJobs

```bash
grpcurl -plaintext -d '{"include_completed": true}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.ListRestoreJobs
```

### CancelRestoreJob

```bash
grpcurl -plaintext -d '{"token": 12345}' \
  $PAGE_SERVER pageservice.v1.PageServiceTestService.CancelRestoreJob
```
