# Compatibility Test macOS Preset

## Goal

Allow the Python compatibility test runner to prepare and build compatibility
branches on macOS, where the Linux-specific `linux-gcc` CMake preset is
disabled.

## Design

Update `test/compatibility/common/compatibility_common.py` to select the
existing `default` CMake preset when running on macOS. Preserve the current
`linux-gcc` selection for branches newer than 7.0 and `linux-v4-gcc` selection
for branches 7.0 and older on Linux.

No CMake preset or Evergreen configuration changes are required. The change is
host-specific and keeps Linux CI behavior unchanged.

## Validation

- Run Python syntax validation for the compatibility helper.
- Configure a compatibility branch with the macOS `default` preset.
- Run the targeted `test_wt13076` compatibility test locally.
- Run `s_fast` for repository checks.
