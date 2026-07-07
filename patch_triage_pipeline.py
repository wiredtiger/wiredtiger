#!/usr/bin/env python3
"""
End-to-end patch build failure triage.

For each failing task in an Evergreen patch, decide whether the failure is a PRE-EXISTING
known issue or a NEW regression introduced by the change, using a tiered signal:

  Tier 1a  Evergreen annotation confirmed BF (task display_status == known-issue)
  Tier 1b  Evergreen annotation suspected BF with confidence >= threshold
  Tier 2   Build Baron open BFG/BF for the failing test
  Tier 3   error-log analysis (WiredTiger relatedness) judged by Claude

Tiers 1 and 2 resolve deterministically in code. Only tests that survive all three
deterministic tiers are sent to Claude for a WT-regression vs infrastructure judgment
and the final human-readable report.

Outputs patch-triage-<id>.json (structured) and triage-<id>.md (rendered by Claude).

Usage:
    python patch_triage_pipeline.py <patch-id-or-url>
    python patch_triage_pipeline.py <patch-id> --no-triage   # skip the Claude step
"""

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from analyze_patch_build import (
    _get_version_project,
    fetch_task_annotation,
    fetch_task_errors,
    fetch_test_error_from_task,
    list_failing_tasks,
    load_evergreen_config,
    parse_patch_id,
    wt_relatedness,
    wt_scan_task_log,
    wt_scan_test_log,
)
from buildbaron_history import check_tests_against_buildbaron, test_name_from_error
from triage_patch_analysis import render_report, run_triage

ROOT = Path(__file__).resolve().parent

# A suspected-issue match at or above this confidence is trusted as pre-existing.
_SUSPECTED_CONFIDENCE_THRESHOLD = 0.9
# A tier-2 Build Baron match below this derived confidence is too weak to settle
# (e.g. no BF ticket link and a variant mismatch) and defers to tier 3.
_BB_CONFIDENCE_THRESHOLD = 0.9
_SCAN_WORKERS = 16


def _banner(title: str) -> None:
    sep = "=" * 60
    print(f"\n{sep}\n  {title}\n{sep}\n")


def _resolve_task_by_annotation(task: dict, token: str, host: str) -> dict | None:
    """Tier 1: resolve a task via its Build Baron annotation, or None if not resolvable."""
    ann = fetch_task_annotation(task["task_id"], token, host)
    if not ann:
        return None
    confirmed = ann.get("confirmed")
    suspected = ann.get("suspected")
    if confirmed:
        return {
            "annotation": ann,
            "verdict": "pre-existing",
            "evidence": f"{confirmed['bf_key']} (confirmed known-issue, conf {confirmed.get('confidence')})",
        }
    if suspected and (suspected.get("confidence") or 0) >= _SUSPECTED_CONFIDENCE_THRESHOLD:
        return {
            "annotation": ann,
            "verdict": "pre-existing",
            "evidence": f"{suspected['bf_key']} (suspected, conf {suspected.get('confidence')})",
        }
    # Annotation exists but suspicion is too weak to settle -- keep it as a hint.
    return {"annotation": ann, "verdict": None, "evidence": ""}


def _build_test_failures(task: dict, bb_results: dict) -> list[dict]:
    """Turn a task's error lines into structured TestFailure records with tier-2/3 signals.

    Tests with no searchable name (pseudo-tests, log-only lines) are tagged
    verdict="not-applicable" so they do not skew the task rollup.
    """
    tests = []
    for err in task.get("errors", []):
        name = test_name_from_error(err)
        bb = bb_results.get(name) if name else None
        if not name:
            verdict, evidence = "not-applicable", ""
        elif bb and bb.get("found") and (bb.get("confidence") or 0) >= _BB_CONFIDENCE_THRESHOLD:
            verdict = "pre-existing"
            ref = bb.get("bf_key") or bb.get("bfg_key")
            evidence = f"{ref} (Build Baron open, conf {bb.get('confidence')})"
        elif bb and bb.get("found"):
            # Weak match (no BF link / variant mismatch) -- let tier 3 judge.
            verdict = "unresolved"
            ref = bb.get("bf_key") or bb.get("bfg_key")
            evidence = f"weak Build Baron match {ref} (conf {bb.get('confidence')})"
        else:
            verdict = "unresolved"
            evidence = ""
        tests.append({
            "error": err,
            "test_name": name,
            "bb_signal": bb,
            "wt_score": wt_relatedness(err),
            "verdict": verdict,
            "evidence": evidence,
        })
    return tests


def _rollup_task_verdict(tests: list[dict]) -> tuple[str, str]:
    """Task verdict from its tests, ignoring not-applicable (pseudo/log-only) entries."""
    decided = [t for t in tests if t["verdict"] != "not-applicable"]
    if not decided:
        return "unknown", "no searchable test signatures"
    if any(t["verdict"] == "unresolved" for t in decided):
        return "unresolved", "one or more tests need tier-3 analysis"
    if all(t["verdict"] == "pre-existing" for t in decided):
        return "pre-existing", "all failing tests match known Build Baron failures"
    return "unresolved", ""


def _attach_bfg_errors(tasks: list[dict], token: str, host: str) -> None:
    """For unresolved tests with a weak Build Baron match, fetch the matched BFG's error."""
    jobs = []
    for t in tasks:
        for test in t.get("tests", []):
            bb = test.get("bb_signal")
            if test["verdict"] == "unresolved" and bb and bb.get("found") and bb.get("bfg_task_id"):
                jobs.append((test, bb))
    if not jobs:
        print("  none")
        return
    with ThreadPoolExecutor(max_workers=_SCAN_WORKERS) as pool:
        futures = {
            pool.submit(
                fetch_test_error_from_task, bb["bfg_task_id"], test["test_name"], token, host
            ): (test, bb)
            for test, bb in jobs
        }
        for future in as_completed(futures):
            test, bb = futures[future]
            try:
                bb["bfg_error"] = future.result()
            except Exception:
                bb["bfg_error"] = ""
            print(f"  [{bb['bfg_key']}] {test['test_name']}: {bb.get('bfg_error', '')[:80] or '(none)'}", flush=True)


def _scan_wt_full_logs(tasks: list[dict], token: str, host: str) -> None:
    """Re-score WT-relatedness over each unresolved test's FULL log.

    The one-line signature misses the stack where WT symbols live, so for unresolved
    tests we scan the whole test log and overwrite the signature-only wt_score.
    """
    def scan(task: dict, test: dict) -> dict:
        # For timeouts the stack is in the task log (hang analyzer), not the per-test log.
        if task.get("display_status") == "task-timed-out":
            wt = wt_scan_task_log(task["task_id"], token, host)
            if wt.get("score"):
                return wt
        return wt_scan_test_log(task["task_id"], test["test_name"], token, host)

    jobs = []
    for t in tasks:
        for test in t.get("tests", []):
            if test["verdict"] == "unresolved" and test.get("test_name"):
                jobs.append((t, test))
    if not jobs:
        print("  none")
        return
    with ThreadPoolExecutor(max_workers=_SCAN_WORKERS) as pool:
        futures = {pool.submit(scan, task, test): test for task, test in jobs}
        for future in as_completed(futures):
            test = futures[future]
            try:
                wt = future.result()
            except Exception:
                wt = None
            if wt and wt.get("scanned") in ("test-log", "task-log"):
                test["wt_score"] = wt
            verdict = "WT-related" if test["wt_score"].get("score") else "not WT"
            print(f"  {test['test_name']}: {verdict} ({','.join(test['wt_score'].get('hits', [])) or 'no signals'})", flush=True)


def build_patch_triage(patch_id: str, cfg: dict) -> dict:
    token, host = cfg["token"], cfg["host"]
    project = _get_version_project(patch_id, token, host)

    print(f"Listing failing tasks for {patch_id} ...")
    tasks = list_failing_tasks(patch_id, token, host)
    if not tasks:
        sys.exit(f"No failing tasks found for patch {patch_id}")
    print(f"Found {len(tasks)} failing task(s)")

    # Tier 1: annotation lookup for every task, in parallel.
    _banner("Tier 1 — Evergreen annotations (confirmed / suspected BF)")
    with ThreadPoolExecutor(max_workers=_SCAN_WORKERS) as pool:
        futures = {
            pool.submit(_resolve_task_by_annotation, t, token, host): t for t in tasks
        }
        for future in as_completed(futures):
            t = futures[future]
            try:
                res = future.result()
            except Exception as exc:
                res = None
                print(f"  [{t['task_name']}] annotation error: {exc}")
            t["annotation"] = res.get("annotation") if res else None
            t["verdict"] = res.get("verdict") if res else None
            t["evidence"] = res.get("evidence") if res else ""
            if t["verdict"]:
                print(f"  [{t['task_name']}] {t['evidence']}", flush=True)

    resolved = [t for t in tasks if t.get("verdict")]
    unresolved = [t for t in tasks if not t.get("verdict")]
    print(f"\n  Tier 1 resolved {len(resolved)}/{len(tasks)} task(s)")

    # Fetch errors only for tasks tier 1 could not settle (defers expensive log I/O).
    _banner("Fetch error signatures for unresolved tasks")
    for t in unresolved:
        print(f"[{t['task_name']}] fetching errors ...")
        try:
            t["errors"] = fetch_task_errors(t["task_id"], token, host)
        except Exception as exc:
            print(f"[{t['task_name']}] ERROR: {exc}")
            t["errors"] = [f"Fetch failed: {exc}"]

    # Tier 2: Build Baron test-name search for the unresolved tasks' tests.
    _banner("Tier 2 — Build Baron known-failure search")
    bb_results = {}
    if unresolved:
        if not project:
            print("  Could not determine project — skipping Build Baron")
        else:
            bb_results = check_tests_against_buildbaron(project, unresolved)

    # Attach tier-2 signals and roll up task verdicts; tier 3 computed as wt_score.
    for t in unresolved:
        t["tests"] = _build_test_failures(t, bb_results)
        verdict, evidence = _rollup_task_verdict(t["tests"])
        t["verdict"] = verdict
        t["evidence"] = evidence

    # For unresolved tests that had a weak Build Baron match, fetch that BFG's actual
    # error so tier 3 can compare signatures (same error -> pre-existing).
    _banner("Fetch known-BFG errors for weak matches (tier-3 comparison)")
    _attach_bfg_errors(unresolved, token, host)

    # Tier 3 pre-filter: score WiredTiger-relatedness over each unresolved test's full log.
    _banner("Scan full logs for WiredTiger relatedness")
    _scan_wt_full_logs(unresolved, token, host)

    # Tasks resolved at tier 1 carry no per-test detail (task-level short-circuit).
    for t in resolved:
        t.setdefault("tests", [])
        t.setdefault("errors", [])

    return {"patch_id": patch_id, "project": project, "tasks": tasks}


def write_json(triage: dict) -> Path:
    out = ROOT / f"patch-triage-{triage['patch_id']}.json"
    out.write_text(json.dumps(triage, indent=2, default=str))
    print(f"\nWrote {out}")
    return out


def print_summary(triage: dict) -> None:
    print("\n--- Triage summary ---")
    counts: dict[str, int] = {}
    for t in triage["tasks"]:
        v = t.get("verdict") or "unknown"
        counts[v] = counts.get(v, 0) + 1
        print(f"  {v:14s} {t['task_name']:55s} {t.get('evidence', '')[:60]}")
    print("\n  " + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tiered triage of Evergreen patch build failures",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("patch", help="Evergreen patch ID or full Spruce/Evergreen URL")
    parser.add_argument(
        "--no-triage",
        action="store_true",
        help="Run the deterministic tiers only; skip the Claude tier-3/synthesis step",
    )
    parser.add_argument(
        "--reuse",
        action="store_true",
        help="Reuse an existing patch-triage-<id>.json instead of re-fetching",
    )
    args = parser.parse_args()

    patch_id = parse_patch_id(args.patch)
    json_path = ROOT / f"patch-triage-{patch_id}.json"

    if args.reuse and json_path.exists():
        print(f"[reuse] loading {json_path}")
        triage = json.loads(json_path.read_text())
    else:
        cfg = load_evergreen_config()
        triage = build_patch_triage(patch_id, cfg)
        write_json(triage)

    print_summary(triage)

    # Always render the clean deterministic report.
    render_report(triage)

    if not args.no_triage:
        _banner("Tier 3 + synthesis — Claude")
        run_triage(triage)


if __name__ == "__main__":
    main()
