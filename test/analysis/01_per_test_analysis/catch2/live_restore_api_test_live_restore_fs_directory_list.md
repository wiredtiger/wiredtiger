# test_live_restore_fs_directory_list — Live restore filesystem directory listing tests

**File:** `test/catch2/live_restore/api/test_live_restore_fs_directory_list.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fs_directory_list` on `WTI_LIVE_RESTORE_FS`
**Test type:** API contract

## TEST_CASE: "Live restore filesystem: directory list" [live_restore_fs]
### SECTION: "dest-only files"
- **What it tests:** Files present only in the destination directory appear in the listing.
- **Components:** `fs_directory_list`
- **Notes:** Source directory is empty.

### SECTION: "source-only files"
- **What it tests:** Files present only in the source directory appear in the unified listing.
- **Components:** `fs_directory_list`
- **Notes:** Destination directory is empty.

### SECTION: "files in both"
- **What it tests:** Files in both source and destination directories are deduplicated in the result.
- **Components:** `fs_directory_list`
- **Notes:** A file with the same name in both locations appears only once.

### SECTION: "mixed (some dest-only, some source-only, some both)"
- **What it tests:** A mix of dest-only, source-only, and shared files produces the correct merged listing.
- **Components:** `fs_directory_list`
- **Notes:** Validates the union logic.

### SECTION: "tombstone hiding"
- **What it tests:** Files with a `.stop` tombstone suffix are excluded from the directory listing.
- **Components:** `fs_directory_list`, tombstone files
- **Notes:** Tombstone files indicate deleted files that should not be visible.

### SECTION: "subfolders"
- **What it tests:** Subdirectory entries within the target path appear in the listing.
- **Components:** `fs_directory_list`
- **Notes:** Directory traversal one level deep.

### SECTION: "ENOENT for missing subfolder"
- **What it tests:** Listing a nonexistent subdirectory returns ENOENT.
- **Components:** `fs_directory_list`
- **Notes:** Error handling for missing paths.

### SECTION: "multi-level subdirectories"
- **What it tests:** A nested subdirectory structure is correctly enumerated.
- **Components:** `fs_directory_list`
- **Notes:** Two-level deep directory traversal.

### SECTION: "subfolder contents"
- **What it tests:** Files inside a subdirectory are returned when that subdirectory is listed.
- **Components:** `fs_directory_list`
- **Notes:** Verifies scoped listing.

### SECTION: "prefix filtering"
- **What it tests:** The `prefix` parameter filters the listing to only files whose names begin with the given prefix.
- **Components:** `fs_directory_list`
- **Notes:** Used by WiredTiger to enumerate specific file types.

### SECTION: "temporary file exclusion"
- **What it tests:** Files with a `.lr_tmp` suffix (live restore temporary files) are excluded from the listing.
- **Components:** `fs_directory_list`, temporary files
- **Notes:** Temporary files are internal; callers should not see them.
