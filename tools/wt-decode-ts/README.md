# wt-decode-ts

A high-performance TypeScript/Node.js port of the WiredTiger page decoding tool. This utility allows you to inspect, decode, and analyze WiredTiger database files (`.wt`), log dumps, and disaggregated storage tables.

## Features

- **Automatic Format Detection**: Automatically identifies if a file is a full WiredTiger file, a fragment, or uses Disaggregated storage format.
- **Rich Terminal Output**: Colorized output for better readability of headers, cells, and timestamps.
- **JSON Mode**: Export decoded page structures to machine-readable JSON for integration with other tools like `jq`.
- **Progress Tracking**: Visual progress bar for large file scans.
- **Library & CLI**: Use it as a standalone command-line tool or as a library in your own Node.js projects.
- **BSON Support**: Optional decoding of cell values as BSON.
- **Compression**: Built-in support for Snappy decompression.

## Installation

### From Source
1. Clone the repository.
2. Navigate to `tools/wt-decode-ts`.
3. Install dependencies:
   ```bash
   npm install
   ```
4. Build the project:
   ```bash
   npm run build
   ```

### Global CLI Link
To use the `wt` command globally:
```bash
npm install -g .
```

## CLI Usage

```bash
wt [options] <filename>
# OR
wt addr <hex_string> [options]
# OR
wt analyze <directory>
```

### Commands

#### `decode` (Default)
Decode a WiredTiger database file or log dump.

#### `addr <hex_string>`
Decode a WiredTiger address cookie. Supports standard and disaggregated formats.

#### `analyze <directory>`
Interactive tool to analyze a WiredTiger home directory.
- Automatically reads `WiredTiger.turtle` and `WiredTiger.wt`.
- Lists all files registered in the metadata.
- Allows selecting a file to view its root page and traverse its B-tree.

### Options (for `decode`)

| Option | Description |
| :--- | :--- |
| `--log-dump` | Input is a hex dump (often found in MongoDB/WiredTiger error logs). |
| `--disagg_table`| Input is a full disagg table in JSONL format. |
| `--json` | Output the decoded structure in machine-readable JSON format. |
| `--raw` | Show raw hex bytes alongside the decoded cell data. |
| `--skip-data` | Only print headers; do not process or print cell data. |
| `--bson` | Attempt to decode cell values as BSON data. |
| `-o, --offset <n>` | Start decoding from a specific byte offset. |
| `-p, --pages <n>` | Limit the number of pages to decode. |
| `--continue` | Continue processing even if a checksum failure is detected. |
| `-b, --bytes` | Show bytes alongside decoding (legacy mode). |
| `-D, --debug` | Enable debug logging. |

### Examples

**Decode a standard .wt file:**
```bash
wt WiredTiger.wt
```

**Analyze a log dump with colorized output:**
```bash
wt --log-dump error.log
```

**Pipe JSON output to jq:**
```bash
wt --json WiredTiger.wt | jq '.[0].pageHeader'
```

## Library Usage

You can import the decoding logic into your own TypeScript projects:

```typescript
import { BinaryFile, WTPage } from 'wt-decode-ts';
import * as fs from 'fs';

const buffer = fs.readFileSync('WiredTiger.wt');
const binFile = new BinaryFile(buffer);

// Parse a page at the current offset
const page = WTPage.parse(binFile, buffer.length, { disagg: false });

if (page.success) {
    console.log(page.pageHeader?.type);
    console.log(page.cells.length);
}
```

## Development

- **Build**: `npm run build`
- **Test**: `npm test`

## License

ISC
