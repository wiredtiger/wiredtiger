# Storage Inspection Path

Use this path for:
- WT home inspection
- MongoDB dbpath inspection
- `.wt` files
- metadata / catalog lookup
- `wt dump`, `wt verify`, `wt stat`, `wt printlog`
- WAL / journal inspection

## Safety

Default to read-only inspection first. Always pass `-r` to the `wt` CLI unless a write
operation has been explicitly authorized.

Do not run destructive `wt` operations (`salvage`, `compact`, `drop`, `truncate`,
`loadtext`) without explicit user approval. See @../reference/safety-rules.md.

## Workflow

### Step 1: Classify the target

- standalone WT home
- MongoDB dbpath
- specific file / URI
- WAL / log question

### Step 2: Start with the smallest safe command

```sh
wt -r -h <dir> list
wt -r -h <dir> stat
wt -r -h <dir> verify <uri>
wt -r -h <dir> printlog
```

If the user only wants an explanation, explain the command instead of running it.

### Step 3: Special handling for MongoDB dbpaths

If the target is a MongoDB dbpath:
- include the log/snappy config in the command
- if the user names a MongoDB namespace, resolve it to its ident first

```sh
wt -r -h /data/db -C "log=(enabled=true,path=journal,compressor=snappy)" list
```

To resolve a namespace to ident, dump `_mdb_catalog`:
```sh
wt -r -h /data/db -C "log=(enabled=true,path=journal,compressor=snappy)" dump table:_mdb_catalog
```

### Step 4: Choose the right subcommand

| Goal | Command |
|---|---|
| Discover what tables exist | `list` |
| Engine or table statistics | `stat` |
| Export data | `dump` |
| Structural integrity check | `verify` |
| WAL / journal inspection | `printlog` |

### Step 5: If output is raw BSON-like bytes

If `wt dump` or `printlog` output contains raw binary / BSON-like bytes:
- do not attempt to interpret them manually
- note the context (table name, log record type) and surface them for further analysis

### Step 6: Escalate carefully

If inspection reveals corruption and the natural next step is salvage or recovery:
- explain exactly what the command rewrites
- explain the risk of data loss
- ask for explicit authorization before continuing

## Output format

### Evidence gathered
- target:
- command:
- notable output:
- interpretation:

### Working theory
One short paragraph.

### Next checks
1. ...
2. ...
