# Decoding Pages & Address Cookies

## Table of Contents
- [Decoding Tool Usage](#decoding-tool-usage)
- [Disagg Block Header](#disagg-block-header)
- [Page Types](#page-types)
- [Decoding Address Cookies](#decoding-address-cookies)

---

## Decoding Tool Usage

```bash
python3 /home/ubuntu/wiredtiger/tools/wt_binary_decode.py --disagg --verbose --bson page.bin
```

| Flag | Purpose |
|------|---------|
| `--disagg` | Parse `WT_BLOCK_DISAGG_HEADER` instead of standard block header |
| `--verbose` | Print cell data contents |
| `--bson` | Decode BSON-encoded values (for catalog, collection pages) |
| `--fragment` | Parse as fragment (use when page is not a complete checkpoint) |

Without `--verbose`, only headers are printed.

---

## Disagg Block Header

Disagg pages use `WT_BLOCK_DISAGG_HEADER` (16 bytes):

```c
struct __wt_block_disagg_header {
   uint8_t magic;               /* 00: 0xdb (full image) or 0xdd (delta) */
   uint8_t version;             /* 01: version of writer */
   uint8_t compatible_version;  /* 02: minimum reader version */
   uint8_t header_size;         /* 03: unencrypted, uncompressed header size */
   uint32_t checksum;           /* 04-07: checksum */
   uint32_t previous_checksum;  /* 08-11: checksum for previous delta or page */
   uint8_t flags;               /* 12: flags */
   uint8_t unused[3];           /* 13-15: padding */
};
```

| Magic | Meaning |
|-------|---------|
| `0xdb` | Full image |
| `0xdd` | Delta |

---

## Page Types

### Internal Page (WT_PAGE_ROW_INT, type 6)

Contains child page addresses as disagg address cookies. Example output:

```
Page Header:
  page type: 6 (WT_PAGE_ROW_INT)
  page flags: PageFlags.WT_PAGE_FT_UPDATE
Block Disagg Header:
  magic: 0xdb (full image)
1: {"version": 0, "min_version": 0, "page_id": 1812, "flags": 0,
    "lsn": 7593161042960580613, "base_lsn": 7593161042960580613,
    "size": 27680, "checksum": 853132641}
```

Each cell's address contains `page_id` + `lsn` for fetching child pages via `GetPageAtLSN`.

### Leaf Page (WT_PAGE_ROW_LEAF, type 7)

Contains actual data. For MongoDB collections, values are BSON documents:

```
Page Header:
  page type: 7 (WT_PAGE_ROW_LEAF)
  page flags: PageFlags.WT_PAGE_EMPTY_V_NONE|WT_PAGE_COMPRESSED
Block Disagg Header:
  magic: 0xdb (full image)
0: desc: 0x5
  short key 1 bytes
1: desc: 0x80
  val 226 bytes
  'ns': 'local.oplog.rs'
```

### Delta Page

Contains `WT_UPDATE` structures. Currently printed as raw bytes (no special decoding):

```
Block Disagg Header:
  magic: 0xdd (delta)
0: desc: 0x25
  short key 9 bytes
1: desc: 0x8c extra: 0x28 runlength/addr: 0 (0x0)
  cell has timestamps:
  ...
```

---

## Decoding Address Cookies

Use `wt_disagg_addr_decode.py` to decode a hex address cookie into page location metadata:

```bash
python3 /home/ubuntu/wiredtiger/tools/wt_disagg_addr_decode.py <hex_address>
```

### Example

Given a checkpoint string:
```
addr="00c09880e869252cb0ffffdfc5e869252cb0ffffdfc5c00a5d4a4c25"
```

Decode:
```bash
python3 /home/ubuntu/wiredtiger/tools/wt_disagg_addr_decode.py \
  00c09880e869252cb0ffffdfc5e869252cb0ffffdfc5c00a5d4a4c25
```

Output: `page_id`, `lsn`, `base_lsn`, `size`, `checksum` — use these to fetch the page via `GetPageAtLSN`.

### Where address cookies appear

| Location | Field | Points to |
|---|---|---|
| Turtle page | `addr` in checkpoint string | Root of shared metadata table |
| Metadata table | `checkpoint` field config strings | Root of each table |
| Internal pages | Cell values | Child leaf/internal pages |
