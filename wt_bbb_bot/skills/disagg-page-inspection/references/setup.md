# Setup & Test State Recreation

## Table of Contents
- [Running SLS Locally](#running-sls-locally)
- [Recreating Persisted Test State](#recreating-persisted-test-state)
- [Finding the Page Server](#finding-the-page-server)
- [Live Environment](#live-environment)
- [Encryption Key (KEK)](#encryption-key-kek)
- [External Documentation](#external-documentation)

---

## Running SLS Locally

Start a local SLS cluster (`--object-services` needed for GetTableAtLSN):

```bash
just sls start --mongod
# or with object services:
just sls start --object-services --mongod
```

Enable verbose logging for page metadata (paliHandleGet/paliHandlePut):

```
--setParameter=logComponentVerbosity="{disaggregatedStorage: 5}"
```

The key fields in log output:

| Field | Type | Description |
|-------|------|-------------|
| `log_id` | uint64 | Shard identifier (locally always 1) |
| `table_id` | uint32 | WiredTiger B-tree ID |
| `page_id` | uint64 | Page identifier within the table |
| `lsn` | uint64 | Log sequence number |

Note the cluster session ID printed on startup (e.g. `sls-1767917748-8199`). It's needed for S3 result access with GetTableAtLSN.

---

## Recreating Persisted Test State

### Evergreen Artifacts

1. Navigate to the Evergreen patch → failing test → **Files** tab.
2. Download the data files (folder named `data NUMBER` containing `db/jobN/mongorunner`).
3. Copy to local machine:
   ```bash
   scp -r "/path/to/Downloads/data 8/db/job4" $HOST:/data/db
   ```
4. Run the recreate script:
   ```bash
   python3 src/mongo/db/modules/atlas/jstests/disagg_storage/libs/recreate.py \
     /data/db/job0/mongorunner/tests/<test_name> \
     --mongod_binary "/home/ubuntu/mongo/bazel-bin/install/bin/mongod"
   ```
5. Wait for `completed services restart` — all services including mongod are then available.

---

## Finding the Page Server

The test artifacts contain `sls_cluster.json` in `/data/db/job0/mongorunner/tests/*/` listing ports for each service. Default local page server: `172.17.0.1:20044`.

To find the address programmatically, parse `sls_cluster.json` for the entry with `svcType: page` and extract its `host:port`.

---

## Live Environment

Follow the [SLS Operations Guide](https://docs.google.com/document/u/0/d/1KcmEFze3g1IsPE2XAURTvkoVoz6zPanRj_UOG53j-_k/edit) for live access. With debug logs enabled, find `GetPageAtLSN` calls:

```json
{
   "level": "DEBUG",
   "log_id": 1254285445831507,
   "lsn": 7578959974114528942,
   "message": "GetPageAtLSN started",
   "page_id": 37735,
   "table_id": 13313,
   "target": "pageservice::server"
}
```

---

## Encryption Key (KEK)

The KEK decrypts per-page DEKs. In dev/test environments it's a hardcoded base64-encoded 256-bit key.

**Locations:**
- Source tree: `src/mongo/db/modules/atlas/src/disagg_storage/test_keyfile.in`
- Test artifacts: `/data/db/job0/mongorunner/decrypt_key`

Must have mode 600:
```bash
chmod 600 /data/db/job0/mongorunner/decrypt_key
```

Some test frameworks generate it dynamically:
```javascript
export function createKeyFile() {
    return writeTempFile("3zKkqoh8BGyC5BnyMZOEXsuTCHTD286SeNXEXeMuMxM=");
}
```

---

## External Documentation

- [SLS Developer Guide](https://docs.google.com/document/u/0/d/1VQQTcjIZp2zqkNxnNIAtiQdGNOFh21762xe4tHJrjCQ/edit)
- [SLS Operations Guide](https://docs.google.com/document/u/0/d/1KcmEFze3g1IsPE2XAURTvkoVoz6zPanRj_UOG53j-_k/edit)
- [Tutorial: Recreating Persisted State](https://docs.google.com/document/u/0/d/1fFA6BU1f9FmTalZR9jn4xEKZ-zzmgTtuVAIcuWr9mFU/edit)
- [MongoD Disagg Storage Dev Guide](https://docs.google.com/document/u/0/d/17loeO2hPuR8IOwnAPhtg1H799HcPndRYZmzkLDMKTMA/edit)
