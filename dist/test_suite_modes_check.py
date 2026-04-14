#!/usr/bin/env python3
#
# Check that changed Python suite tests have an explicit suite-mode decision.
#
# Intended usage: run in CI tasks like s_all and s_fast to flag when a branch
# changes/adds tests under test/suite/ that should be considered for inclusion
# in manifest-backed modes (e.g. plaid).
#
# Policy:
# - If a test module is changed/added and is NOT included in a manifest-backed
#   mode, it must have an explicit annotation that records the decision
#   (e.g. "plaid:exclude ..."). This avoids repeated failures once the decision
#   is made.
# - The annotation is stored in the test file itself so it stays close to the
#   change and remains repeatable.
#
# This script is deterministic given the git diff and the files' annotations.
#

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RE_SUITE_BLOCK_START = re.compile(r"^\s*#\s*\[SUITE_MODES\]\s*$")
RE_SUITE_BLOCK_END = re.compile(r"^\s*#\s*\[END_SUITE_MODES\]\s*$")
# Example line:  # plaid:exclude reason="needs tiered extensions"
RE_SUITE_LINE = re.compile(
    r"^\s*#\s*(?P<mode>[a-zA-Z0-9_-]+)\s*:\s*(?P<decision>include|exclude)\b(?P<rest>.*)$"
)


@dataclass(frozen=True)
class ModeManifest:
    name: str
    included_modules: set[str]


def _repo_root() -> Path:
    r = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(r.stdout.strip()).resolve()


def _git_diff_names(base: str, head: str, paths: Iterable[str]) -> list[tuple[str, str]]:
    """
    Return list of (status, path) where status is one of A/M/D/R...
    """
    cmd = ["git", "diff", "--name-status", f"{base}...{head}", "--"]
    cmd.extend(paths)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr or "")
        raise SystemExit(r.returncode)
    out: list[tuple[str, str]] = []
    for raw in r.stdout.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split("\t")
        status = parts[0].strip()
        # rename: R100\told\tnew
        if status.startswith("R") and len(parts) >= 3:
            out.append((status, parts[2]))
        elif len(parts) >= 2:
            out.append((status, parts[1]))
    return out


def _load_suite_manifests(repo: Path) -> list[ModeManifest]:
    suite_modes_py = repo / "test" / "suite" / "suite_modes.py"
    if not suite_modes_py.is_file():
        return []

    # Import by path to avoid relying on python module layout.
    import importlib.util

    spec = importlib.util.spec_from_file_location("wt_suite_modes", suite_modes_py)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)  # type: ignore

    manifests: list[ModeManifest] = []
    included = set(getattr(m, "PLAUD_TESTS", ()))
    if included:
        manifests.append(ModeManifest(name="plaid", included_modules=set(included)))
    return manifests


def _parse_suite_annotations(path: Path) -> dict[str, str]:
    """
    Parse suite mode decisions from a file. Returns {mode: decision}.
    """
    try:
        text = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}

    in_block = False
    decisions: dict[str, str] = {}
    for line in text[:200]:  # only inspect file header region
        if RE_SUITE_BLOCK_START.match(line):
            in_block = True
            continue
        if RE_SUITE_BLOCK_END.match(line):
            break
        if not in_block:
            continue
        m = RE_SUITE_LINE.match(line)
        if not m:
            continue
        mode = m.group("mode").strip().lower()
        decision = m.group("decision").strip().lower()
        decisions[mode] = decision
    return decisions


def _suggest_annotation_block(mode: str, decision: str, reason: str | None) -> str:
    if reason:
        reason = reason.replace('"', "'").strip()
        extra = f' reason="{reason}"'
    else:
        extra = ""
    return "\n".join(
        [
            "# [SUITE_MODES]",
            f"# {mode}:{decision}{extra}",
            "# [END_SUITE_MODES]",
            "",
        ]
    )


def _apply_annotation(path: Path, mode: str, decision: str, reason: str | None) -> bool:
    """
    Insert a suite-modes block near the top if none exists; otherwise append a line inside it.
    Returns True if file changed.
    """
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)

    # Find an existing block.
    start_i = end_i = None
    for i, line in enumerate(lines[:300]):
        if RE_SUITE_BLOCK_START.match(line):
            start_i = i
            continue
        if start_i is not None and RE_SUITE_BLOCK_END.match(line):
            end_i = i
            break

    if start_i is not None and end_i is not None:
        # Insert a decision line just before END.
        insert_at = end_i
        new_line = f"# {mode}:{decision}"
        if reason:
            new_line += f' reason="{reason.replace(chr(34), chr(39)).strip()}"'
        new_line += "\n"
        lines.insert(insert_at, new_line)
        path.write_text("".join(lines), encoding="utf-8")
        return True

    # No block: insert after shebang (if any) and contiguous header comments.
    insert_at = 0
    if lines and lines[0].startswith("#!"):
        insert_at = 1
    # Skip initial blank/comment lines to keep header tidy.
    while insert_at < len(lines):
        s = lines[insert_at].strip()
        if s == "" or s.startswith("#"):
            insert_at += 1
            continue
        break

    block = _suggest_annotation_block(mode, decision, reason)
    lines.insert(insert_at, block)
    path.write_text("".join(lines), encoding="utf-8")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Check suite-mode decisions for changed test/suite modules.")
    ap.add_argument("--base", default=os.environ.get("WT_SUITE_CHECK_BASE", "HEAD~1"))
    ap.add_argument("--head", default="HEAD")
    ap.add_argument(
        "--mode",
        action="append",
        default=["plaid"],
        help="Manifest-backed mode(s) to enforce (repeatable). Default: plaid",
    )
    ap.add_argument(
        "--new-only",
        action="store_true",
        default=True,
        help="Only check newly added tests (status A, and renames into test/suite/). Default: true",
    )
    ap.add_argument(
        "--all-changed",
        dest="new_only",
        action="store_false",
        help="Check all changed tests (added + modified).",
    )
    ap.add_argument("--apply", action="store_true", help="Apply exclude annotations to changed files that need them.")
    ap.add_argument("--reason", default="not in short suite", help="Reason string used with --apply.")
    args = ap.parse_args()

    repo = _repo_root()
    os.chdir(repo)

    manifests = {m.name: m for m in _load_suite_manifests(repo)}
    enforce_modes = [m.strip().lower() for m in (args.mode or [])]
    enforce_modes = sorted(set(m for m in enforce_modes if m in manifests))
    if not enforce_modes:
        print("No manifest-backed modes available to enforce; nothing to do.")
        return 0

    changed = _git_diff_names(args.base, args.head, ["test/suite"])
    changed_tests: list[tuple[str, Path, str]] = []
    for status, rel in changed:
        p = Path(rel)
        if p.name.startswith("test_") and p.suffix == ".py":
            if args.new_only:
                # Only consider brand new tests. Treat renames into test/suite/ as new.
                # git diff --name-status uses:
                #   A <path>
                #   R<score> <old> <new>
                is_new = status == "A" or status.startswith("R")
                if not is_new:
                    continue
            modname = p.stem
            changed_tests.append((status, repo / p, modname))

    if not changed_tests:
        if args.new_only:
            print("No new test/suite test_*.py files.")
        else:
            print("No changed test/suite test_*.py files.")
        return 0

    failures: list[str] = []
    applied = 0

    for status, path, modname in sorted(changed_tests, key=lambda t: t[2]):
        ann = _parse_suite_annotations(path)
        for mode in enforce_modes:
            in_manifest = modname in manifests[mode].included_modules
            decision = ann.get(mode)
            if in_manifest:
                continue
            if decision in ("exclude", "include"):
                continue
            msg = f"{path.relative_to(repo)}: changed ({status}); not in {mode} manifest and missing [SUITE_MODES] decision"
            if args.apply:
                _apply_annotation(path, mode=mode, decision="exclude", reason=args.reason)
                applied += 1
            else:
                failures.append(msg)

    if args.apply:
        print(f"Applied suite-mode annotations to {applied} file(s).")
        # After apply, do not fail in this run; the diff will be visible to the developer/CI.
        return 0

    if failures:
        print("Suite mode inclusion check failed.")
        for f in failures:
            print(" - " + f)
        print()
        print("To acknowledge the decision, add a block like:")
        print()
        print(_suggest_annotation_block("plaid", "exclude", "not in short suite"))
        print("Or re-run with --apply to insert default exclude annotations.")
        return 1

    print("Suite mode inclusion check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

