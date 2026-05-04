# Copyright Analysis

## Canonical formats for WiredTiger-owned files

### C / C++ / H (dual copyright, standard header)
```
/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
```

### C / C++ / H (dual copyright, BSD-style redistribution)
```
/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
```

### Python / Shell (public domain with dual copyright)
```
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
```

### Python / Shell (standard copyright, dual)
```
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.
```

## Rules
- **MongoDB, Inc.** copyright year range must be exactly **`2014-present`**
  (2014 = year MongoDB acquired WiredTiger; "present" = open-ended as of any release)
- **WiredTiger, Inc.** copyright year range must be exactly **`2008-2014`**
  (WiredTiger became a MongoDB subsidiary in 2014, so WiredTiger's copyright ended that year)

## Third-party copyright formats (expected, not checked)

These are in bundled/vendored code and should NOT be modified:

| Holder | Example |
|--------|---------|
| Apple Inc. | `Copyright (c) 2015 Apple Inc. All rights reserved.` |
| Google | `Copyright (c) 2011 Google, Inc.` |
| IBM | `Copyright IBM Corp. 2015` / `Copyright (C) 2017 Rogerio Alves ...` |
| testtools developers | `Copyright (c) 2008-2017 testtools developers.` |
| Canonical Ltd | `Copyright (C) 2005-2011 Canonical Ltd` |
| Robert Collins | `Copyright (C) 2005 Robert Collins ...` |
| BSD utilities | `Copyright (c) 1987, 1993, 1994` |

**Note:** `test/3rdparty/python-subunit-1.4.4/.../subunit_filter.py` contains a typo:
`Copyright (C) 200-2013` (missing digit in start year). This is in vendored code so
the correct fix is to update the vendored copy or suppress, not change the original author's copyright.

## Known special cases (intentionally non-standard)

| File | Reason |
|------|--------|
| `src/docs/style/footer.html` | Public-facing web page shows `2008-present MongoDB` — intentional display range |
| `src/utilities/util_cpyright.c` line 38 | Not a header copyright; it is code that prints a copyright string |

## Violations found (as of 2026-05-01)

### Stale end year ("2020" instead of "present")
| File | Incorrect line |
|------|----------------|
| `src/block_cache/block_cache.c` | `Copyright (c) 2014-2020 MongoDB, Inc.` |
| `src/include/block_cache.h` | `Copyright (c) 2014-2020 MongoDB, Inc.` |
| `test/suite/test_prepare_hs05.py` | `Public Domain 2014-2020 MongoDB, Inc.` |

### Wrong start year for MongoDB (should be 2014, not the file-creation year)
| File | Incorrect line |
|------|----------------|
| `src/include/futex.h` | `Copyright (c) 2024-present MongoDB, Inc.` |
| `src/os_linux/os_futex.c` | `Copyright (c) 2024-present MongoDB, Inc.` |
| `src/os_win/os_futex.c` | `Copyright (c) 2024-present MongoDB, Inc.` |
| `src/os_darwin/os_futex.c` | `Copyright (c) 2024-present MongoDB, Inc.` |
| `src/checkpoint/checkpoint_stats.c` | `Copyright (c) 2025-present MongoDB, Inc.` |
| `tools/checksum_bitflip/checksum_bitflip.c` | `Public Domain 2024-present MongoDB, Inc.` |

### Wrong end year for WiredTiger (should be 2014)
| File | Incorrect line |
|------|----------------|
| `src/include/int4bitpack_inline.h` | `Copyright (c) 2008-present WiredTiger, Inc.` |
| `test/packing/int4bpack-test.c` | `Public Domain 2008-present WiredTiger, Inc.` |
| `src/docs/tools/doxfilter` | `Public Domain 2008-2012 WiredTiger, Inc.` |
| `src/docs/tools/pyfilter` | `Public Domain 2008-2012 WiredTiger, Inc.` |

## CI check

The check is implemented in `dist/s_copyright_format.py` and registered as the
`s-copyright-format` task in `test/evergreen_develop.yml` under the `infrequent-checks`
buildvariant (batchtime: 1440 — runs once a day).

It is deliberately NOT part of `s_all` because:
- `s_all` is run on every PR and the existing `s_copyright` (release-only) covers format checks
- This check is complementary: it catches wrong year ranges in all environments regardless of release mode
- It is cheap (no network calls) but unlikely to regress often, so daily is sufficient
