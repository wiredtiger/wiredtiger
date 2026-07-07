#!/usr/bin/env python3
"""
Check failing tests against Build Baron's known-failure database.

Build Baron clusters CI failures across the whole project into Build Failure Groups
(BFGs) and links them to BF Jira tickets. Querying it for a test tells us whether the
failure is already tracked as an open, known issue -- strong evidence that a patch-build
failure is pre-existing rather than introduced by the change.

Requires the bb-client package (private):
    pip install 'git+ssh://git@github.com/10gen/build-baron-client.git'
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from bb_client import BuildBaronClient, BBSearchBfgsSpec, get_oauth_credentials
from bb_client.models.bfg import AttributeType as AT

_BB_WORKERS = 12
# Per-HTTP-call timeout. The Build Baron backend can be slow for some test names; without a
# cap the client retries against its 120s default and a few slow queries stall the whole batch.
_BB_HTTP_TIMEOUT = 20
# One attempt: a timeout marks the test distinctly (defers to tier 3) rather than retrying,
# which kept determinism but stacked latency. Determinism comes from the stable sort below.
_BB_SEARCH_ATTEMPTS = 1

# Pseudo-tests that are resmoke fixture machinery, not real tests. They match nearly
# every failure and add noise, so we skip them.
_PSEUDO_TEST_RE = re.compile(
    r"(fixture_teardown|fixture_setup|job\d+_|:CheckReplDBHash|:CheckMetadataConsistency"
    r"|:ValidateCollections|:CheckMultikeyConsistency|:CheckReplDBHashInBackground)",
    re.IGNORECASE,
)


def _make_client() -> BuildBaronClient:
    creds = get_oauth_credentials(False)  # False = print URL, do not open a browser
    client = BuildBaronClient.new_client(creds)
    # Force a short timeout on every HTTP call so slow queries fail fast instead of
    # retrying against the client's 120s default.
    session = getattr(client, "_session", None)
    if session is not None:
        original = session.request

        def _timed_request(*args, **kwargs):
            kwargs.setdefault("timeout", _BB_HTTP_TIMEOUT)
            return original(*args, **kwargs)

        session.request = _timed_request
    return client


def test_name_from_error(error_line: str) -> str | None:
    return _test_name_from_error(error_line)


def _test_name_from_error(error_line: str) -> str | None:
    """Extract a Build-Baron-searchable test name from an analysis error line.

    Error lines look like "jstests/sharding/foo.js (exit -1): ...". Build Baron matches
    on the bare test name, so we reduce to the basename without extension.
    """
    m = re.match(r"^(.+?) \(exit", error_line.strip())
    if not m:
        return None
    raw = m.group(1).strip().replace("\\", "/")
    base = raw.split("/")[-1]
    if base.endswith(".js"):
        base = base[:-3]
    # Hook pseudo-tests come through as "or8:CheckReplDBHash" -- keep the base test.
    if ":" in base:
        base = base.split(":")[0]
    if _PSEUDO_TEST_RE.search(raw):
        return None
    return base or None


# Bounded scan: enough BFGs to gauge recurrence and variant overlap without paginating
# the hundreds a generic failure can carry. Larger than the representative we show so the
# scanned set (and thus the derived confidence) is stable across runs for typical tests.
_MAX_BFG_SCAN = 10


def _bfg_sort_key(bfg) -> tuple:
    """Deterministic ordering: BF-linked first, then newest commit, then key.

    Build Baron does not guarantee result order, so we sort to pick a stable
    representative and keep confidence reproducible across runs.
    """
    bf_linked = 1 if bfg.attributes.get(AT.BF_KEY) else 0
    order = bfg.attributes.get(AT.REVISION_ORDER) or "0"
    try:
        order_val = int(order)
    except (TypeError, ValueError):
        order_val = 0
    return (-bf_linked, -order_val, bfg.get_key() or "")


def _derive_confidence(bf_linked: bool, variant_match: bool) -> float:
    """Proxy confidence for a tier-2 match. Build Baron BFGs carry no native score,
    so we approximate: a match grouped into a BF ticket and hitting the same variant
    as the patch failure is the strongest signal.
    """
    score = 0.5
    if bf_linked:
        score += 0.3
    if variant_match:
        score += 0.2
    return round(score, 2)


def _search_one(
    client: BuildBaronClient, project: str, test: str, patch_variants: set[str]
) -> dict:
    """Return an open-BFG record for a test, or {found: False} if none.

    Scans a bounded number of BFGs to gauge recurrence and whether any match the same
    build variant the patch failed on, which feeds the derived confidence.
    """
    spec = BBSearchBfgsSpec(
        projects=[project], tests=[test], status="Open", page_size=_MAX_BFG_SCAN
    )
    bfgs = None
    last_exc = None
    for _ in range(_BB_SEARCH_ATTEMPTS):
        try:
            collected = []
            for bfg in client.search_bfgs(spec):
                collected.append(bfg)
                if len(collected) >= _MAX_BFG_SCAN:
                    break
            bfgs = collected
            break
        except Exception as exc:
            last_exc = exc
    if bfgs is None:
        # A timeout must be distinct from "no match": treating a slow query as a
        # regression would flip the verdict run-to-run. Mark it so the pipeline defers.
        return {"found": False, "error": type(last_exc).__name__}
    if not bfgs:
        return {"found": False}

    # Sort so the set and its representative are reproducible across runs.
    bfgs.sort(key=_bfg_sort_key)
    matched = [
        {
            "bfg_key": b.get_key(),
            "bf_key": b.attributes.get(AT.BF_KEY),
            "variant": b.get_build_variant(),
            "failure_type": b.get_failure_type(),
            "task_id": getattr(b, "task_id", ""),
            "execution": getattr(b, "execution", 0),
        }
        for b in bfgs
    ]
    rep = matched[0]
    bf_key = rep["bf_key"] or next((m["bf_key"] for m in matched if m["bf_key"]), None)
    variant_match = bool(patch_variants & {m["variant"] for m in matched})
    return {
        "found": True,
        "bfgs": matched,          # all matching BFGs (sorted), for full evidence
        "bfg_key": rep["bfg_key"],  # representative, for the deterministic verdict
        "bf_key": bf_key,
        "variant": rep["variant"],
        "failure_type": rep["failure_type"],
        "summary": bfgs[0].attributes.get(AT.SUMMARY),
        "count": len(matched),
        "confidence": _derive_confidence(bool(bf_key), variant_match),
        # The Evergreen task behind the representative BFG, used to fetch its error.
        "bfg_task_id": rep["task_id"],
        "bfg_execution": rep["execution"],
    }


def check_tests_against_buildbaron(
    project: str, failing_results: list[dict]
) -> dict[str, dict]:
    """For each failing test, check Build Baron for an open known-failure BFG.

    Returns {test_name: {"found": bool, "bfg_key", "bf_key", "confidence", ...}}. The
    derived confidence uses whether the patch failed the test on a variant Build Baron
    also has an open BFG for.
    """
    # test_name -> the build variants this test failed on in the current patch
    test_variants: dict[str, set[str]] = {}
    for r in failing_results:
        variant = r.get("build_variant", "")
        for err in r.get("errors", []):
            name = _test_name_from_error(err)
            if name:
                test_variants.setdefault(name, set()).add(variant)

    if not test_variants:
        print("  No searchable test names found")
        return {}

    print(f"  Querying Build Baron for {len(test_variants)} test(s) (project {project})")
    client = _make_client()

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_BB_WORKERS) as pool:
        futures = {
            pool.submit(_search_one, client, project, t, v): t
            for t, v in sorted(test_variants.items())
        }
        for future in as_completed(futures):
            test = futures[future]
            try:
                info = future.result()
            except Exception as exc:
                info = {"found": False, "error": type(exc).__name__}
                print(f"  [bb] {test}: ERROR {type(exc).__name__}", flush=True)
            results[test] = info
            if info.get("found"):
                ref = info.get("bf_key") or info.get("bfg_key")
                print(f"  [bb] {test}: KNOWN ({ref}, conf {info['confidence']})", flush=True)
            elif "error" not in info:
                print(f"  [bb] {test}: no open BFG (candidate regression)", flush=True)

    return results
