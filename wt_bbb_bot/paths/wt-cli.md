# WT CLI path

Use this path for:
- "how do I use wt"
- ".wt file"
- "show me what's in this WiredTiger directory"
- dump / stat / verify / printlog
- MongoDB dbpath inspection at the storage-engine level

# Safety

Default to read-only inspection. Do not execute write-like commands without explicit user confirmation.

# Workflow

## Step 1: Classify the target

Determine whether the target is:
- a standalone WiredTiger home
- a MongoDB dbpath
- a specific WT URI or file
- a WAL / printlog question

## Step 2: Start with the smallest safe command

Typical starting points:
```sh
wt -r -h <dir> list
wt -r -h <dir> stat
wt -r -h <dir> verify <uri>
wt -r -h <dir> printlog
```

If the user only wants explanation, explain the command instead of running it.

## Step 3: Special handling for MongoDB dbpaths

If the target is a MongoDB dbpath:
- include the log/snappy config in the command
- if the user names a MongoDB namespace, resolve the namespace to its ident first before operating on the file

Typical config flag:
```
-C "log=(enabled=true,path=journal,compressor=snappy)"
```

Example:
```sh
wt -r -h /data/db -C "log=(enabled=true,path=journal,compressor=snappy)" list
```

To resolve a namespace to ident, dump `_mdb_catalog`:
```sh
wt -r -h /data/db -C "log=(enabled=true,path=journal,compressor=snappy)" dump table:_mdb_catalog
```

## Step 4: Choose the right subcommand

| Goal | Command |
|---|---|
| Discover what tables exist | `list` |
| Engine or table statistics | `stat` |
| Export data | `dump` |
| Structural integrity check | `verify` |
| WAL / journal inspection | `printlog` |

## Step 5: Escalate carefully

If inspection reveals corruption and the natural next step is salvage or recovery:
- explain exactly what the command rewrites
- explain the risk of data loss
- ask for explicit authorization before continuing

# Output format

## Current understanding
One short paragraph.

## Evidence gathered
- command:
- notable output:
- what it means:

## Working theory
One short paragraph.

## Next checks
1. ...
2. ...

## Exact commands
```sh
# commands here
```
