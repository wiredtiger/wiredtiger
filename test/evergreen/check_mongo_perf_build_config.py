#!/usr/bin/env python3
"""Check that WT uses the profile flags from MongoDB's sys-perf build."""

import argparse
import re
import shlex
import sys
from pathlib import Path


MONGO_VARIANT = "amazon2023-arm64-compile"
MONGO_TASK_GROUP = "compile_and_package_serial_no_unittests_TG"
MONGO_COMPILE_TASK = "archive_dist_test"
WT_COMPILE_FUNCTION = "compile mongodb"


def fail(message):
    raise ValueError(message)


def list_entry(contents, name):
    """Return a two-space-indented YAML list entry containing the given name."""
    lines = contents.splitlines()
    starts = [
        index
        for index, line in enumerate(lines)
        if re.match(r"^  -\s", line) is not None
    ]
    for start_index, start in enumerate(starts):
        end = starts[start_index + 1] if start_index + 1 < len(starts) else len(lines)
        block = lines[start:end]
        if any(
            re.match(rf"^\s+name:\s*{re.escape(name)}\s*$", line)
            or re.match(rf"^  -\s*name:\s*{re.escape(name)}\s*$", line)
            for line in block
        ):
            return "\n".join(block)
    fail(f"Could not find YAML list entry named {name!r}")


def mapping_entry(contents, name):
    """Return one two-space-indented YAML mapping entry."""
    lines = contents.splitlines()
    start = next(
        (
            index
            for index, line in enumerate(lines)
            if re.match(rf'^  "{re.escape(name)}"\s*:', line)
        ),
        None,
    )
    if start is None:
        fail(f"Could not find YAML mapping entry {name!r}")

    end = len(lines)
    for index in range(start + 1, len(lines)):
        if re.match(r'^  "[^\"]+"\s*:', lines[index]):
            end = index
            break
    return "\n".join(lines[start:end])


def folded_scalar_flags(block, name):
    """Read command-line flags from a folded YAML scalar."""
    lines = block.splitlines()
    for index, line in enumerate(lines):
        match = re.match(rf"^(\s*){re.escape(name)}:\s*>-\s*$", line)
        if match is None:
            continue
        indent = len(match.group(1))
        values = []
        for value_line in lines[index + 1 :]:
            if not value_line.strip():
                continue
            value_indent = len(value_line) - len(value_line.lstrip())
            if value_indent <= indent:
                break
            values.append(value_line.strip())
        return shlex.split(" ".join(values))
    fail(f"Could not find folded YAML scalar {name!r}")


def bazel_build_flags(block):
    """Return the arguments from a multiline `bazel build` shell command."""
    lines = block.splitlines()
    for index, line in enumerate(lines):
        if re.match(r"^\s*bazel build(?:\s|$)", line) is None:
            continue
        command_parts = []
        current = index
        while current < len(lines):
            part = lines[current].strip()
            continued = part.endswith("\\")
            command_parts.append(part[:-1].rstrip() if continued else part)
            current += 1
            if not continued:
                break
        tokens = shlex.split(" ".join(command_parts))
        if tokens[:2] != ["bazel", "build"]:
            fail("Found a malformed bazel build command in the WT compile function")
        return tokens[2:]
    fail("Could not find the bazel build command in the WT compile function")


def shared_perf_flags(flags):
    """Keep settings shared by the perf builds, excluding target/distro flags."""
    # MongoDB's gdb-index and MONGO_DISTMOD flags are output/platform-specific.
    return [flag for flag in flags if flag.startswith("--config=") or flag.startswith("--release=")]


def enterprise_override(flags):
    """Return the last explicit Enterprise override, if either build has one."""
    override = None
    prefixes = ("--build_enterprise=", "--//bazel/config:build_enterprise=")
    for flag in flags:
        for prefix in prefixes:
            if flag.startswith(prefix):
                override = flag[len(prefix) :].lower()
    return override


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mongo_root", type=Path, help="path to the 10gen/mongo checkout")
    args = parser.parse_args()

    mongo_root = args.mongo_root
    wt_root = Path(__file__).resolve().parents[2]

    variant_file = mongo_root / "etc/evergreen_yml_components/variants/amazon/test_dev.yml"
    compile_file = mongo_root / "etc/evergreen_yml_components/tasks/compile_tasks_shared.yml"
    wt_evergreen_file = wt_root / "test/evergreen.yml"

    variant = list_entry(variant_file.read_text(), MONGO_VARIANT)
    if MONGO_TASK_GROUP not in variant:
        fail(f"MongoDB sys-perf variant {MONGO_VARIANT!r} no longer uses {MONGO_TASK_GROUP!r}")

    compile_yaml = compile_file.read_text()
    task_group = list_entry(compile_yaml, MONGO_TASK_GROUP)
    if not re.search(rf"^\s+-\s+{re.escape(MONGO_COMPILE_TASK)}\s*$", task_group, re.MULTILINE):
        fail(f"MongoDB task group {MONGO_TASK_GROUP!r} no longer compiles {MONGO_COMPILE_TASK!r}")

    mongo_compile_task = list_entry(compile_yaml, MONGO_COMPILE_TASK)
    mongo_flags = folded_scalar_flags(mongo_compile_task, "bazel_args")
    mongo_flags.extend(folded_scalar_flags(variant, "bazel_compile_flags"))

    wt_function = mapping_entry(wt_evergreen_file.read_text(), WT_COMPILE_FUNCTION)
    wt_flags = bazel_build_flags(wt_function)

    mongo_profile = shared_perf_flags(mongo_flags)
    wt_profile = shared_perf_flags(wt_flags)
    if mongo_profile != wt_profile:
        print("MongoDB sys-perf profile flags: " + " ".join(mongo_profile), file=sys.stderr)
        print("WT many-collection profile flags: " + " ".join(wt_profile), file=sys.stderr)
        fail("WiredTiger's MongoDB perf build profile differs from MongoDB sys-perf")

    # Both builds use the same MongoDB .bazelrc, so matching explicit overrides
    # ensures they inherit the same default build_enterprise setting.
    if enterprise_override(mongo_flags) != enterprise_override(wt_flags):
        fail("WiredTiger and MongoDB sys-perf use different explicit build_enterprise overrides")

    print("MongoDB sys-perf and WT share profile flags: " + " ".join(mongo_profile))
    print("Explicit build_enterprise overrides match")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
