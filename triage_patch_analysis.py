#!/usr/bin/env python3
"""
Claude tier-3 judgment + synthesis for patch build triage.

Takes the structured triage object produced by patch_triage_pipeline.py, where tiers 1
and 2 have already settled most tasks deterministically. Claude only judges the tests
still marked "unresolved" (WiredTiger regression vs infrastructure vs unknown) using the
error signature and a WiredTiger-relatedness score, then writes triage-<id>.md.

Usage (normally invoked by patch_triage_pipeline.py):
    python triage_patch_analysis.py <patch-id>   # loads patch-triage-<id>.json

Requirements:
    pip install anthropic
    Set ANTHROPIC_API_KEY, or set GROVE_API_KEY to use MongoDB's internal Grove gateway.
"""

import json
import os
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

import anthropic

ROOT = Path(__file__).resolve().parent

_GROVE_BASE_URL = "https://grove-gateway-prod.azure-api.net/grove-foundry-prod/anthropic"
_JIRA_BASE = "https://jira.mongodb.org/browse"
_BB_BFG_BASE = "https://buildbaron.corp.mongodb.com/ui/#/bfg"


def _bf_link(bf_key: str) -> str:
    return f"[{bf_key}]({_JIRA_BASE}/{bf_key})"


def _bfg_link(bfg_key: str) -> str:
    return f"[{bfg_key}]({_BB_BFG_BASE}/{bfg_key})"


def _issue_link(key: str) -> str:
    """Link a BF key to Jira, a BFG key to Build Baron."""
    return _bfg_link(key) if key.startswith("BFG-") else _bf_link(key)


def _wt_judgement(wt: dict) -> str:
    """WiredTiger-relatedness judgement from HIGH-precision signals, with reasoning."""
    wt = wt or {}
    hits = wt.get("hits") or []
    ambient = wt.get("ambient") or []
    if hits:
        return f"WT-related: **yes** — {', '.join(hits)} (in the failure/stack)"
    if ambient:
        return (
            "WT-related: **no** — only ambient WiredTiger logging "
            f"({', '.join(ambient)}), no WT frames in the crash/stack"
        )
    return "WT-related: **no** — no WiredTiger signals found"


_MAX_BFG_REFS = 5


def _bfg_refs(bb: dict, limit: int = _MAX_BFG_REFS) -> str:
    """Render up to `limit` matching BFG/BF links from a tier-2 signal."""
    matched = bb.get("bfgs") or [{"bfg_key": bb.get("bfg_key"), "bf_key": bb.get("bf_key")}]
    links = []
    for m in matched[:limit]:
        links.append(_bf_link(m["bf_key"]) if m.get("bf_key") else _bfg_link(m.get("bfg_key", "?")))
    extra = len(matched) - limit
    if extra > 0:
        links.append(f"+{extra} more")
    return ", ".join(links)


def _core_signature(error: str) -> str:
    """Strip resmoke/log prefixes and volatile tokens so two errors can be compared."""
    s = re.sub(r"^\[[^\]]*\]\s*", "", error.strip())        # [js_test:foo] prefix
    s = re.sub(r"^\w+\d*\|\s*", "", s)                        # sh1234| prefix
    s = re.sub(r'\{"t":\{"\$date".*', "", s)                 # trailing structured log
    s = re.sub(r"\b\d{4}-\d{2}-\d{2}T[\d:.+]+Z?\b", "", s)   # timestamps
    s = re.sub(r"\b(tid|thread)\s*\d+\b", "", s, flags=re.I)  # thread ids
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


def _signature_match_note(patch_error: str, bfg_error: str) -> str:
    """Deterministic comparison of patch failure vs known-BFG failure signatures."""
    if not bfg_error:
        return "known BFG error unavailable for comparison"
    a, b = _core_signature(patch_error), _core_signature(bfg_error)
    if not a or not b:
        return "insufficient signature to compare"
    ratio = SequenceMatcher(None, a, b).ratio()
    if ratio >= 0.85:
        return "error signature matches the known failure"
    if ratio >= 0.5:
        return f"error signature partially matches (similarity {ratio:.0%})"
    return f"error signature differs from the known failure (similarity {ratio:.0%})"


def _task_header(t: dict) -> str:
    return f"**{t['task_name']} ({t.get('build_variant', '')})**"


def _annotation_ref(t: dict) -> tuple[str, str] | None:
    """Return (bf_link, kind) from a task's annotation, or None."""
    ann = t.get("annotation") or {}
    conf = ann.get("confirmed")
    susp = ann.get("suspected")
    if conf and conf.get("bf_key"):
        return _issue_link(conf["bf_key"]), f"confirmed known-issue, conf {conf.get('confidence')}"
    if susp and susp.get("bf_key"):
        return _issue_link(susp["bf_key"]), f"suspected, conf {susp.get('confidence')}"
    return None


def _render_pre_existing(t: dict) -> list[str]:
    lines = []
    ann = _annotation_ref(t)
    if ann:
        link, kind = ann
        lines.append(f"- {_task_header(t)} -> {link} ({kind})")
        return lines
    # Resolved by tier-2 Build Baron per test.
    for test in t.get("tests", []):
        if test["verdict"] != "pre-existing":
            continue
        bb = test.get("bb_signal") or {}
        lines.append(
            f"- {_task_header(t)} / `{test.get('test_name')}` -> {_bfg_refs(bb)} "
            f"(conf {bb.get('confidence')})"
        )
    return lines


def _render_unresolved(t: dict) -> list[str]:
    lines = []
    for test in t.get("tests", []):
        if test["verdict"] != "unresolved":
            continue
        bb = test.get("bb_signal") or {}
        wt = test.get("wt_score") or {}
        if bb.get("found"):
            refs = _bfg_refs(bb)
            note = _signature_match_note(test.get("error", ""), bb.get("bfg_error", ""))
            lines.append(
                f"- {_task_header(t)} / `{test.get('test_name')}` -> {refs} "
                f"(conf {bb.get('confidence')}, {note})"
            )
        else:
            lines.append(
                f"- {_task_header(t)} / `{test.get('test_name') or '(no test)'}` -> "
                f"no known failure match — candidate new regression"
            )
        lines.append(f"  - {_wt_judgement(wt)}")
    return lines


def render_report(triage: dict) -> Path:
    """Write a clean human-readable triage-<id>.md directly from the structured object."""
    patch_id = triage["patch_id"]
    tasks = triage["tasks"]
    counts: dict[str, int] = {}
    for t in tasks:
        counts[t.get("verdict") or "unknown"] = counts.get(t.get("verdict") or "unknown", 0) + 1

    lines = [f"# Triage report: `{patch_id}`\n"]
    lines.append(f"Project: {triage.get('project', '?')}  |  Failing tasks: {len(tasks)}\n")
    lines.append("**Verdicts:** " + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())) + "\n")

    pre = [ln for t in tasks if t.get("verdict") == "pre-existing" for ln in _render_pre_existing(t)]
    unres = [ln for t in tasks if t.get("verdict") == "unresolved" for ln in _render_unresolved(t)]
    unknown = [t for t in tasks if t.get("verdict") == "unknown"]

    if unres:
        lines.append("\n## Needs investigation (unresolved)\n")
        lines.extend(unres)
    if pre:
        lines.append("\n## Pre-existing known failures\n")
        lines.extend(pre)
    if unknown:
        lines.append("\n## Unknown (no searchable signature)\n")
        lines.extend(f"- {_task_header(t)}" for t in unknown)

    out_path = ROOT / f"triage-{patch_id}.md"
    out_path.write_text("\n".join(lines) + "\n")
    print(f"\nWrote {out_path}")
    return out_path


def _make_client() -> anthropic.Anthropic:
    grove_key = os.environ.get("GROVE_API_KEY")
    if grove_key:
        return anthropic.Anthropic(
            base_url=_GROVE_BASE_URL,
            api_key=grove_key,
            default_headers={"api-key": grove_key},
        )
    return anthropic.Anthropic()


_SYSTEM_PROMPT = """\
You are a WiredTiger/MongoDB build failure triage assistant.

Most failing tasks in this patch have ALREADY been classified deterministically:
- pre-existing: matched to an open Build Failure (BF) ticket or Build Baron group (BFG),
  either via the Evergreen task annotation or a Build Baron search. These are settled.
- unresolved: no known-failure match found. These are the ones you must judge.

For each UNRESOLVED test, decide whether the failure is a NEW regression plausibly caused
by a WiredTiger change, or something else. Use the error signature and the provided
WiredTiger-relatedness score (WT signals found in the error text).

Some unresolved tests carry a WEAK Build Baron match: a same-named known failure (BFG/BF)
exists but below the confidence bar. When its error text is provided, COMPARE it to the
failure signature: if they describe the same failure, the verdict is pre-existing (cite the
BFG/BF); if they clearly differ, it is a candidate new-regression.

Do NOT change any verdict that is already pre-existing. Only classify unresolved items.

Write a triage report in markdown:

## Summary
A short table: counts of pre-existing vs new-regression vs infrastructure vs unknown.

## New regressions (investigate)
For each unresolved test you judge a regression:
- **<task> / <test>** - verdict: new-regression | infrastructure | unknown
  - reasoning: one sentence
  - WT-related: yes/no (from the WT score)

## Pre-existing (known failures)
Bulleted list: <task> -> <BF/BFG evidence>. One line each. Do not elaborate.

Verdict guidance for unresolved items:
- new-regression   - error looks like a real test/product failure, especially with WT signals
- infrastructure   - S3, network, timeout with no stack, symbolizer, provisioning
- unknown          - insufficient signal
"""


def _format_triage_for_prompt(triage: dict) -> str:
    lines = [f"Patch: {triage['patch_id']}  (project {triage.get('project', '?')})\n"]
    for t in triage["tasks"]:
        lines.append(f"\n## {t['task_name']} ({t.get('build_variant', '')})")
        lines.append(f"- task verdict so far: {t.get('verdict')}")
        if t.get("evidence"):
            lines.append(f"- evidence: {t['evidence']}")
        ann = t.get("annotation")
        if ann and ann.get("message"):
            lines.append(f"- annotation: {ann['message'][:300]}")
        for test in t.get("tests", []):
            if test["verdict"] == "pre-existing":
                lines.append(f"  - [PRE-EXISTING] {test['error'][:160]}  ({test['evidence']})")
            else:
                wt = test.get("wt_score") or {}
                lines.append(
                    f"  - [UNRESOLVED] {test['error'][:200]}  "
                    f"(WT score {wt.get('score', 0)}: {','.join(wt.get('hits', []))})"
                )
                bb = test.get("bb_signal") or {}
                if bb.get("found"):
                    ref = bb.get("bf_key") or bb.get("bfg_key")
                    bfg_err = bb.get("bfg_error")
                    if bfg_err:
                        lines.append(
                            f"      known {ref} (conf {bb.get('confidence')}) error was: {bfg_err[:200]}"
                        )
                        lines.append(
                            "      -> if this matches the failure above, verdict pre-existing; if different, new-regression"
                        )
                    else:
                        lines.append(
                            f"      a weak {ref} match exists (conf {bb.get('confidence')}) but its error could not be fetched"
                        )
    return "\n".join(lines)


def _has_unresolved(triage: dict) -> bool:
    return any(
        t.get("verdict") == "unresolved"
        or any(x.get("verdict") == "unresolved" for x in t.get("tests", []))
        for t in triage["tasks"]
    )


def run_triage(triage: dict) -> Path:
    patch_id = triage["patch_id"]
    out_path = ROOT / f"triage-{patch_id}.md"

    client = _make_client()
    grove_key = os.environ.get("GROVE_API_KEY")
    model = "claude-opus-4-6" if grove_key else "claude-opus-4-8"
    extra = {} if grove_key else {"thinking": {"type": "adaptive"}}

    unresolved = _has_unresolved(triage)
    print(f"Triaging patch {patch_id} ({'has' if unresolved else 'no'} unresolved tests) ...")

    with client.messages.stream(
        model=model,
        max_tokens=8000,
        **extra,
        system=[
            {"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
        ],
        messages=[{"role": "user", "content": _format_triage_for_prompt(triage)}],
    ) as stream:
        for text in stream.text_stream:
            print(text, end="", flush=True)
        response = stream.get_final_message()

    triage_text = "".join(b.text for b in response.content if b.type == "text")
    out_path.write_text(f"# Triage report: `{patch_id}`\n\n{triage_text}")
    print(f"\n\nWrote {out_path}")
    return out_path


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("Usage: python triage_patch_analysis.py <patch-id>")
    patch_id = sys.argv[1]
    json_path = ROOT / f"patch-triage-{patch_id}.json"
    if not json_path.exists():
        sys.exit(f"No structured triage at {json_path} — run patch_triage_pipeline.py first")
    run_triage(json.loads(json_path.read_text()))


if __name__ == "__main__":
    main()
