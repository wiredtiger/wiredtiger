# Compatibility macOS Preset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Python compatibility runner build branches locally on macOS by selecting the existing `default` CMake preset.

**Architecture:** Keep branch and toolchain selection unchanged, but choose the CMake preset from the host platform. macOS uses `default`; Linux continues to use `linux-gcc` or `linux-v4-gcc` based on branch age.

**Tech Stack:** Python 3, CMake presets, Ninja, WiredTiger compatibility test runner.

---

## File Map

- Modify: `test/compatibility/common/compatibility_common.py` to select the host-appropriate CMake preset.
- Test: `test/compatibility/suite/test_wt13076.py` through the existing compatibility runner.

### Task 1: Add Host-Aware Preset Selection

**Files:**
- Modify: `test/compatibility/common/compatibility_common.py:29,123-130`
- Test: `test/compatibility/suite/test_wt13076.py`

- [ ] **Step 1: Add the platform dependency**

Change the import line to include `platform`:

```python
import os, platform, re, shutil, sys
```

- [ ] **Step 2: Select the macOS preset before Linux branch selection**

Replace the current unconditional preset selection:

```python
if branch.startswith('mongodb-') and \
        int(branch.split('-')[1].split('.')[0]) <= 7:
    preset = 'linux-v4-gcc'
else:
    preset = 'linux-gcc'
```

with:

```python
if platform.system() == 'Darwin':
    preset = 'default'
elif branch.startswith('mongodb-') and \
        int(branch.split('-')[1].split('.')[0]) <= 7:
    preset = 'linux-v4-gcc'
else:
    preset = 'linux-gcc'
```

- [ ] **Step 3: Run syntax and formatting checks**

Run:

```bash
python3 -m py_compile test/compatibility/common/compatibility_common.py
git diff --check
```

Expected: both commands exit successfully.

- [ ] **Step 4: Run the targeted compatibility test**

Run from the compatibility suite directory:

```bash
python3 -c 'import compatibility_test; compatibility_test.run("test_wt13076")'
```

Expected: the runner configures the prepared branches with the macOS `default` preset and executes the WT-13076 upgrade/downgrade scenarios. If an existing stale empty `build.compatibility` directory blocks setup, remove only that generated empty directory and rerun.

- [ ] **Step 5: Run repository fast checks**

Run:

```bash
./s_fast
```

Expected: `s_all run finished. Error? 0`.

- [ ] **Step 6: Commit the implementation**

```bash
git add test/compatibility/common/compatibility_common.py
git commit -m "WT-13076 support compatibility tests on macOS"
```
