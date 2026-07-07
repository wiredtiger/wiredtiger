#!/usr/bin/env python3
"""
Fetch failing task logs from an Evergreen patch build and extract error signatures.

Usage:
    python analyze_patch_build.py <patch-id-or-url>

Outputs patch-analysis-<patch_id>.md with one section per failing task.

Examples:
    python analyze_patch_build.py 6700abc1234567890abcdef0
    python analyze_patch_build.py "https://spruce.mongodb.com/version/6700abc1234567890abcdef0"
"""

import argparse
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parent

DEFAULT_EVERGREEN_HOST = "https://evergreen.mongodb.com"
EVERGREEN_CONFIG_PATH = Path.home() / ".evergreen.yml"

_MAX_ERRORS = 20
_SCAN_WORKERS = 16

# Fallback grep pattern — used when a task has no structured test results.
_ERROR_RE = re.compile(
    r"(failed to load|invariant|fassert|invalid access|BACKTRACE[^L]|\d+ thread threw"
    r"|checkReplicatedDataHashes|assert\.js|__wt_abort|uncaught exception)",
    re.IGNORECASE,
)

_NOISE_PATTERNS = [
    re.compile(r"botocore\.exceptions\."),
    re.compile(r"boto3\."),
    re.compile(r"S3\.Client\.exceptions\."),
    re.compile(r"HeadObject operation"),
    re.compile(r"mciuploads\.s3\.amazonaws\.com"),
    re.compile(r"403.*Forbidden"),
    re.compile(r"Forbidden.*403"),
    re.compile(r"urllib3\."),
    re.compile(r"requests\.packages\.urllib3"),
    re.compile(r"File \"/"),
    re.compile(r"^\s+\^+\s*$"),
    re.compile(r"@src/mongo/shell/assert\.js:\d+"),
    re.compile(r"^['\"]?PATH="),                  # env dump preceding a shell invocation
    re.compile(r"verbatim resmoke\.py invocation"),  # resmoke command echo, not a failure
    re.compile(r"Sometime after completion of JSTest"),  # teardown-timing notice
    re.compile(r"No failure logs/stacktrace files found"),  # symbolizer notice, not the failure
]


def parse_patch_id(raw: str) -> str:
    raw = raw.strip().rstrip("/")
    for pattern in (
        r"spruce\.mongodb\.com/version/([a-f0-9]+)",
        r"evergreen\.mongodb\.com/version/([a-f0-9]+)",
        r"/version/([a-f0-9]+)",
    ):
        m = re.search(pattern, raw)
        if m:
            return m.group(1)
    return raw


def load_evergreen_config() -> dict:
    if not EVERGREEN_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Evergreen config not found at {EVERGREEN_CONFIG_PATH}")
    with open(EVERGREEN_CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    host = cfg.get("api_server_host", DEFAULT_EVERGREEN_HOST).rstrip("/")
    raw = subprocess.check_output(
        ["evergreen", "client", "get-oauth-token"], text=True
    )
    token = next(
        (line.strip() for line in raw.splitlines() if line.strip().startswith("eyJ")),
        None,
    )
    if not token:
        raise RuntimeError(
            f"No JWT token found in `evergreen client get-oauth-token` output.\n"
            f"Complete the device auth flow first:\n{raw}"
        )
    return {"token": token, "host": host}


def _evergreen_get(url: str, token: str) -> dict | list:
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    resp.raise_for_status()
    return resp.json()


# Only these display statuses are genuine test failures worth triaging. System/setup
# failures (shown purple in the Evergreen UI) are infrastructure noise and are ignored.
# known-issue is kept so tier 1 can report it as pre-existing.
_TRIAGE_DISPLAY_STATUSES = {"failed", "task-timed-out", "known-issue"}


def _check_build(build: dict, token: str, host: str) -> list[dict]:
    tasks = _evergreen_get(
        f"{host}/rest/v2/builds/{build['_id']}/tasks?status=failed", token
    )
    results = []
    for task in tasks:
        if task.get("display_only"):
            continue
        if task.get("display_status", "") not in _TRIAGE_DISPLAY_STATUSES:
            continue
        results.append({
            "task_id": task.get("task_id", ""),
            "task_name": task.get("display_name") or task.get("task_name", ""),
            "build_variant": task.get("build_variant", ""),
            "display_status": task.get("display_status", ""),
        })
    return results


def list_failing_tasks(patch_id: str, token: str, host: str) -> list[dict]:
    """Return real (non-display) failed tasks across all builds.

    A patch that is still running holds failed tasks in builds not yet marked failed
    (status started/created), so we scan every build rather than only failed ones. Builds
    with no failing tasks return quickly from the status=failed task query.
    """
    all_builds = _evergreen_get(f"{host}/rest/v2/versions/{patch_id}/builds", token)
    print(f"  {len(all_builds)} builds total, scanning all for failing tasks")

    # Dedup by (task_name, build_variant): a task retried across executions appears with
    # multiple task_ids. Prefer the known-issue execution (Evergreen's authoritative verdict).
    by_key: dict[tuple[str, str], dict] = {}
    with ThreadPoolExecutor(max_workers=_SCAN_WORKERS) as pool:
        futures = {pool.submit(_check_build, b, token, host): b for b in all_builds}
        for future in as_completed(futures):
            build = futures[future]
            variant = build.get("build_variant", build["_id"])
            try:
                tasks = future.result()
            except Exception as exc:
                print(f"  ERROR checking {variant}: {exc}")
                continue
            for t in tasks:
                key = (t["task_name"], t["build_variant"])
                existing = by_key.get(key)
                if existing is None or (
                    t["display_status"] == "known-issue"
                    and existing["display_status"] != "known-issue"
                ):
                    by_key[key] = t

    results = list(by_key.values())
    for t in results:
        print(f"  {t['display_status'].upper()}: {t['task_name']} ({t['build_variant']})")
    return results


_TEST_LOG_WALL_TIMEOUT = 60  # seconds before giving up on a single test log


def _fetch_test_log_lines(log_url: str, token: str) -> list[str]:
    """Fetch all lines of a test-specific raw log, with a wall-clock timeout."""
    if not log_url:
        return []
    try:
        t0 = time.monotonic()
        with requests.get(
            log_url,
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=(10, 30),
        ) as resp:
            resp.raise_for_status()
            lines: list[str] = []
            for raw in resp.iter_lines(decode_unicode=True):
                if time.monotonic() - t0 > _TEST_LOG_WALL_TIMEOUT:
                    print(f"  (test log timeout after {_TEST_LOG_WALL_TIMEOUT}s)", flush=True)
                    break
                line = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                lines.append(line)
        return lines
    except Exception:
        return []


# Matches structured mongo log severity field anywhere in a line (including prefixed lines
# like "[resmoke] {...}"). Severity I=info, D=debug are skipped; E/W/F are errors.
_MONGO_JSON_SEVERITY_RE = re.compile(r'"s"\s*:\s*"([IDWEF])"')


def _extract_error_from_log(lines: list[str]) -> str:
    """Pick the most informative error line from a test log.

    Scans the full log for _ERROR_RE matches first (catches backtraces that appear
    before the resmoke summary at the tail). Falls back to the last non-trivial,
    non-informational line when no pattern matches.
    """
    candidates_error: list[str] = []
    candidates_fallback: list[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if any(p.search(stripped) for p in _NOISE_PATTERNS):
            continue

        sev_match = _MONGO_JSON_SEVERITY_RE.search(stripped)
        if sev_match and sev_match.group(1) in ("I", "D"):
            continue

        if _ERROR_RE.search(stripped) and not re.search(r"@src/mongo/shell/", stripped):
            candidates_error.append(stripped[:300])
        else:
            candidates_fallback.append(stripped[:300])

    if candidates_error:
        return candidates_error[0]
    if candidates_fallback:
        return candidates_fallback[-1]
    return ""


def _fetch_failed_tests(task_id: str, token: str, host: str) -> list[dict]:
    """Return structured failed-test records from the Evergreen test results API."""
    data = _evergreen_get(
        f"{host}/rest/v2/tasks/{task_id}/tests?status=fail&limit={_MAX_ERRORS}",
        token,
    )
    if not isinstance(data, list):
        return []
    results = []
    for t in data:
        logs = t.get("logs", {})
        results.append({
            "test_file": t.get("test_file", ""),
            "exit_code": t.get("exit_code", -1),
            "log_url_raw": logs.get("url_raw", "") or logs.get("url", ""),
        })
    return results


def _test_log_lines_by_basename(
    task_id: str, test_basename: str, token: str, host: str
) -> list[str]:
    """Fetch the full log lines of a specific failed test within a task, matched by basename."""
    if not task_id:
        return []
    try:
        tests = _fetch_failed_tests(task_id, token, host)
    except Exception:
        return []
    for t in tests:
        base = t["test_file"].replace("\\", "/").split("/")[-1]
        if base.endswith(".js"):
            base = base[:-3]
        if base == test_basename:
            return _fetch_test_log_lines(t["log_url_raw"], token)
    return []


# Cap the WT log scan: crash stacks live near the failure, so a bounded scan that
# stops at the first high-precision hit avoids streaming 100k+ line logs in full.
_WT_SCAN_MAX_LINES = 20000


def _wt_scan_stream(url: str, token: str, scanned_label: str) -> dict:
    """Stream a log and score WT-relatedness, stopping at the first high-precision hit.

    Bounded by _WT_SCAN_MAX_LINES and the shared wall-clock timeout so a huge log does
    not stall the scan. Ambient signals are still collected over what was read.
    """
    if not url:
        return {"score": 0, "hits": [], "ambient": [], "scanned": "none"}
    high_hits: list[str] = []
    ambient: set[str] = set()
    t0 = time.monotonic()
    n = 0
    try:
        with requests.get(
            url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=(10, 30)
        ) as resp:
            resp.raise_for_status()
            for raw in resp.iter_lines(decode_unicode=True):
                n += 1
                if n > _WT_SCAN_MAX_LINES or time.monotonic() - t0 > _TEST_LOG_WALL_TIMEOUT:
                    break
                line = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                for pat, label in _WT_SIGNAL_HIGH:
                    if pat.search(line):
                        high_hits.append(label)
                if not high_hits:  # only bother with ambient until a high hit decides it
                    for pat, label in _WT_SIGNAL_AMBIENT:
                        if pat.search(line):
                            ambient.add(label)
                if high_hits:
                    break  # verdict decided; no need to read the rest
    except Exception:
        pass
    return {
        "score": len(high_hits),
        "hits": sorted(set(high_hits)),
        "ambient": sorted(ambient),
        "scanned": scanned_label,
    }


def wt_scan_test_log(task_id: str, test_basename: str, token: str, host: str) -> dict:
    """Score WiredTiger-relatedness over a test's log, bounded and early-exiting.

    The one-line error signature misses the stack where WT symbols appear, so we scan the
    test log, but stop at the first crash/stack WT signal rather than reading it all.
    """
    if not task_id:
        return {"score": 0, "hits": [], "ambient": [], "scanned": "none"}
    try:
        tests = _fetch_failed_tests(task_id, token, host)
    except Exception:
        return {"score": 0, "hits": [], "ambient": [], "scanned": "none"}
    for t in tests:
        base = t["test_file"].replace("\\", "/").split("/")[-1]
        if base.endswith(".js"):
            base = base[:-3]
        if base == test_basename:
            return _wt_scan_stream(t["log_url_raw"], token, "test-log")
    return {"score": 0, "hits": [], "ambient": [], "scanned": "none"}


def wt_scan_task_log(task_id: str, token: str, host: str) -> dict:
    """Score WT-relatedness over the task log (hang-analyzer backtrace for timeouts)."""
    url = f"{host}/rest/v2/tasks/{task_id}/build/TaskLogs?type=task_log"
    return _wt_scan_stream(url, token, "task-log")


def fetch_test_error_from_task(
    task_id: str, test_basename: str, token: str, host: str
) -> str:
    """Return the error signature of a specific test within a task, matched by basename.

    Used to pull a known BFG's actual error so tier 3 can compare it against the patch
    failure (same error -> pre-existing, different -> possible new regression).
    """
    if not task_id:
        return ""
    try:
        tests = _fetch_failed_tests(task_id, token, host)
    except Exception:
        return ""
    for t in tests:
        base = t["test_file"].replace("\\", "/").split("/")[-1]
        if base.endswith(".js"):
            base = base[:-3]
        if base != test_basename:
            continue
        lines = _fetch_test_log_lines(t["log_url_raw"], token)
        return _extract_error_from_log(lines) if lines else ""
    return ""


def _task_errors_via_test_results(task_id: str, token: str, host: str) -> list[str] | None:
    """Return per-test error strings using the test results API.

    Returns None if no structured test results exist (task failed before tests ran).
    """
    tests = _fetch_failed_tests(task_id, token, host)
    if not tests:
        return None
    errors: list[str] = []
    for t in tests:
        test_file = t["test_file"]
        exit_code = t["exit_code"]
        log_lines = _fetch_test_log_lines(t["log_url_raw"], token)
        error_msg = _extract_error_from_log(log_lines)
        if error_msg:
            errors.append(f"{test_file} (exit {exit_code}): {error_msg}")
        else:
            errors.append(f"{test_file} (exit {exit_code}): no error message extracted")
        print(f"  [test] {test_file} -> {error_msg[:100] or '(no msg)'}", flush=True)
    return errors


def _task_errors_via_log(task_id: str, token: str, host: str) -> list[str]:
    """Fallback: stream the task log and grep for error patterns."""
    url = f"{host}/rest/v2/tasks/{task_id}/build/TaskLogs?type=task_log"
    print("  No test results — streaming task log ...", flush=True)
    t0 = time.monotonic()
    with requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        stream=True,
        timeout=(10, 60),
    ) as resp:
        resp.raise_for_status()
        lines: list[str] = []
        seen: set[str] = set()
        errors: list[str] = []
        last_report = t0
        for raw in resp.iter_lines(decode_unicode=True):
            line = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
            lines.append(line)
            now = time.monotonic()
            if now - last_report >= 3:
                print(f"  ... {len(lines):,} lines ({now - t0:.0f}s)", flush=True)
                last_report = now
            stripped = line.strip()
            if (
                _ERROR_RE.search(stripped)
                and not any(p.search(stripped) for p in _NOISE_PATTERNS)
            ):
                key = stripped[:300]
                if key not in seen:
                    seen.add(key)
                    errors.append(key)
                    print(f"  [error {len(errors)}] {stripped[:120]}", flush=True)
                    if len(errors) >= _MAX_ERRORS:
                        print(f"  Reached {_MAX_ERRORS} errors — stopping early")
                        break
    print(f"  Done: {len(lines):,} lines in {time.monotonic() - t0:.1f}s, {len(errors)} error(s)")
    return errors


def fetch_task_errors(task_id: str, token: str, host: str) -> list[str]:
    """Return error signatures for a task: test results API first, log grep fallback."""
    errors = _task_errors_via_test_results(task_id, token, host)
    if errors is not None:
        return errors
    return _task_errors_via_log(task_id, token, host)


def _get_version_project(patch_id: str, token: str, host: str) -> str:
    try:
        data = _evergreen_get(f"{host}/rest/v2/versions/{patch_id}", token)
        return data.get("project_identifier") or data.get("project") or ""
    except Exception:
        return ""


# Annotation suggestions can reference a BF ticket or a BFG group; keep both.
_ISSUE_KEY_RE = re.compile(r"^(BF|BFG)-\d+$")

# WiredTiger signals, split by precision. HIGH signals indicate WT in the failure/crash
# context (stack frames, aborts, panics) and are what the WT-related verdict is based on.
# AMBIENT signals appear in essentially every mongod log (WiredTiger is always the storage
# engine) so they are recorded for context but never flip the verdict on their own.
_WT_SIGNAL_HIGH = [
    (re.compile(r"__wt_abort"), "__wt_abort (crash)"),
    (re.compile(r"\bWT_PANIC\b"), "WT_PANIC"),
    (re.compile(r"__wti?_\w+\s*\("), "WiredTiger symbol in stack"),
    (re.compile(r"src/third_party/wiredtiger/"), "WiredTiger source frame"),
    (re.compile(r"src/mongo/db/storage/wiredtiger/"), "WT storage-layer frame"),
    (re.compile(r"\[WT_VERB_|WiredTiger error|WT_ERROR\b"), "WiredTiger error"),
]
_WT_SIGNAL_AMBIENT = [
    (re.compile(r'"c"\s*:\s*"(WT|STORAGE)"'), "WT/STORAGE log line"),
    (re.compile(r"wiredtiger_open"), "wiredtiger_open (startup)"),
    (re.compile(r"\bWT_[A-Z_]+\b"), "WT_ code"),
    (re.compile(r"WiredTiger", re.IGNORECASE), "WiredTiger mentioned"),
]


def _issue_record(entry: dict) -> dict | None:
    """Reduce an annotation issue entry to {bf_key, confidence, jira_url}.

    Keeps BF and BFG keys; skips task_id "same-as-this-run" pointers. The field is named
    bf_key for continuity but may hold a BFG key.
    """
    key = entry.get("issue_key", "")
    if not _ISSUE_KEY_RE.match(key):
        return None
    return {
        "bf_key": key,
        "confidence": entry.get("confidence_score"),
        "jira_url": entry.get("url", ""),
    }


def _best_issue(entries: list) -> dict | None:
    """Highest-confidence valid (BF/BFG) issue record from an annotation array."""
    records = [r for e in (entries or []) if (r := _issue_record(e))]
    if not records:
        return None
    return max(records, key=lambda r: r.get("confidence") or 0)


def fetch_task_annotation(task_id: str, token: str, host: str) -> dict | None:
    """Return the task's Build Baron annotation, or None if it has no BF/BFG match.

    Splits the two annotation arrays: `issues[]` are confirmed matches (task is marked
    known-issue) and `suspected_issues[]` are auto-suggested matches carrying a confidence
    score. BF and BFG keys are kept; the highest-confidence one wins.
    """
    try:
        data = _evergreen_get(f"{host}/rest/v2/tasks/{task_id}/annotations", token)
    except Exception:
        return None
    if not isinstance(data, list) or not data:
        return None
    ann = data[0]

    confirmed = _best_issue(ann.get("issues"))
    suspected = _best_issue(ann.get("suspected_issues"))
    if not confirmed and not suspected:
        return None

    bb_url = next(
        (m.get("url", "") for m in ann.get("metadata_links") or [] if m.get("url")), ""
    )
    return {
        "confirmed": confirmed,
        "suspected": suspected,
        "message": (ann.get("note") or {}).get("message", ""),
        "bb_url": bb_url,
    }


def wt_relatedness(error_text: str) -> dict:
    """Judge WiredTiger-relatedness from failure text.

    `score`/`hits` count only HIGH-precision signals (WT in the crash/stack), which is
    what the WT-related verdict is based on. `ambient` records the always-present WT
    logging signals for context but does not affect the verdict.
    """
    hits = [label for pat, label in _WT_SIGNAL_HIGH if pat.search(error_text)]
    ambient = [label for pat, label in _WT_SIGNAL_AMBIENT if pat.search(error_text)]
    return {"score": len(hits), "hits": hits, "ambient": ambient}


def generate_report(results: list[dict], patch_id: str) -> Path:
    out_path = ROOT / f"patch-analysis-{patch_id}.md"
    lines = [
        f"# Patch build analysis: `{patch_id}`\n",
        f"Failing tasks: {len(results)}\n",
    ]
    for entry in results:
        errors = entry["errors"]
        lines.append(f"\n## {entry['task_name']} ({entry['build_variant']})\n")
        lines.append(f"**task_id:** `{entry['task_id']}`\n")
        if errors:
            lines.append("\n```\n")
            lines.extend(e + "\n" for e in errors)
            lines.append("```\n")
        else:
            lines.append("_No error signature extracted — see full log._\n")

    out_path.write_text("".join(lines))
    print(f"\nWrote {out_path}")
    print("\n--- Terminal summary ---")
    for entry in results:
        first_err = entry["errors"][0] if entry["errors"] else "(no signature)"
        print(f"  {entry['task_name']:50s}  {first_err[:100]}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch failing task logs from an Evergreen patch build",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("patch", help="Patch ID or full Evergreen/Spruce URL")
    args = parser.parse_args()

    patch_id = parse_patch_id(args.patch)
    print(f"Patch ID: {patch_id}")

    print("Loading Evergreen config ...")
    cfg = load_evergreen_config()

    print(f"Listing failing tasks ...")
    tasks = list_failing_tasks(patch_id, cfg["token"], cfg["host"])
    if not tasks:
        sys.exit(f"No failing tasks found for patch {patch_id}")
    print(f"Found {len(tasks)} failing task(s)")

    results = []
    for task in tasks:
        print(f"\n[{task['task_name']}] fetching errors ...")
        try:
            errors = fetch_task_errors(task["task_id"], cfg["token"], cfg["host"])
            print(f"[{task['task_name']}] {len(errors)} error(s)")
        except Exception as exc:
            print(f"[{task['task_name']}] ERROR: {exc}")
            errors = [f"Fetch failed: {exc}"]
        results.append({**task, "errors": errors})

    generate_report(results, patch_id)


if __name__ == "__main__":
    main()
