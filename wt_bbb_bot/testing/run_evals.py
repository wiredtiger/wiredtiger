#!/usr/bin/env python3
"""
run_evals.py — LLM-as-judge eval runner for bug-bash-bot.

Two eval types:
  scenario  — self-contained prompt (no live MCP needed, never goes stale)
  fixture   — real ticket prompt replayed against recorded MCP responses

For each eval:
  1. Runs `claude -p "/bug-bash-bot <prompt>"` (agent under test)
  2. Optionally runs a judge call that scores 1–5 against expected_behavior
  3. Saves output + judgment to testing/evals/results/YYYY-MM-DD/eval-{id}.md
  4. Prints a summary table

Usage:
    python testing/run_evals.py
    python testing/run_evals.py --ids 1,2,3
    python testing/run_evals.py --type scenario
    python testing/run_evals.py --no-judge
    python testing/run_evals.py --timeout 900
"""

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
EVALS_FILE = REPO_ROOT / "testing" / "evals" / "evals.json"
SCENARIOS_DIR = REPO_ROOT / "testing" / "evals" / "scenarios"
FIXTURES_BASE = REPO_ROOT / "testing" / "evals" / "fixtures"
RESULTS_BASE = REPO_ROOT / "testing" / "evals" / "results"
MOCK_SERVER = REPO_ROOT / "testing" / "mock_mcp_server.py"

SKILL_CMD = "/bug-bash-bot"
DEFAULT_TIMEOUT = 900
JUDGE_TIMEOUT = 60

JUDGE_PROMPT = """\
You are evaluating the output of an AI agent that triages WiredTiger build failure tickets.

Expected behavior:
{expected_behavior}

Agent output:
---
{agent_output}
---

Score the agent output on a scale of 1–5:
  5 = fully meets expected behavior
  4 = mostly correct, minor omissions
  3 = partially correct, key steps missing
  2 = significant gaps or wrong approach
  1 = completely wrong or no useful output

Reply in this exact format:
SCORE: <1-5>
REASON: <one paragraph>
"""

log = logging.getLogger("run_evals")


def setup_logging(results_dir: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(results_dir / "run.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------------------------------------------------------------------------
# Agent runners
# ---------------------------------------------------------------------------

def run_scenario(prompt: str, timeout: int) -> tuple[str, str]:
    """Run against a self-contained prompt with no MCP tools."""
    try:
        result = subprocess.run(
            ["claude", "-p", f"{SKILL_CMD} {prompt}",
             "--allowedTools", "Bash,Read,Write,Edit,Agent"],
            capture_output=True, text=True, timeout=timeout,
        )
        output = result.stdout
        if result.returncode != 0 and result.stderr:
            output += f"\n\n---\nstderr:\n{result.stderr.strip()}"
        return output, "ok" if result.returncode == 0 else "error"
    except subprocess.TimeoutExpired:
        return f"TIMED OUT after {timeout // 60} min", "timeout"
    except FileNotFoundError:
        log.critical("claude CLI not found")
        sys.exit(1)


def run_fixture(prompt: str, fixture_dir: Path, timeout: int) -> tuple[str, str]:
    """Run against recorded fixture MCP responses via mock MCP server."""
    if not fixture_dir.exists():
        return (
            f"SKIPPED — fixture directory not found: {fixture_dir}\n"
            f"Record fixtures with: python testing/record_fixtures.py {fixture_dir.name}",
            "skipped",
        )

    mcp_config = {
        "mcpServers": {
            "devprod-mcp-gateway": {
                "command": sys.executable,
                "args": [str(MOCK_SERVER), "--fixture-dir", str(fixture_dir)],
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(mcp_config, f)
        config_path = f.name

    try:
        result = subprocess.run(
            ["claude", "-p", f"{SKILL_CMD} {prompt}", "--mcp-config", config_path],
            capture_output=True, text=True, timeout=timeout,
        )
        output = result.stdout
        if result.returncode != 0 and result.stderr:
            output += f"\n\n---\nstderr:\n{result.stderr.strip()}"
        return output, "ok" if result.returncode == 0 else "error"
    except subprocess.TimeoutExpired:
        return f"TIMED OUT after {timeout // 60} min", "timeout"
    except FileNotFoundError:
        log.critical("claude CLI not found")
        sys.exit(1)
    finally:
        Path(config_path).unlink(missing_ok=True)


def run_agent(eval_: dict, timeout: int) -> tuple[str, str]:
    eval_type = eval_.get("type", "scenario")

    if eval_type == "scenario":
        scenario_file = SCENARIOS_DIR / eval_["scenario_file"].split("/")[-1]
        prompt = scenario_file.read_text()
        return run_scenario(prompt, timeout)

    elif eval_type == "fixture":
        fixture_dir = FIXTURES_BASE / eval_["fixture_dir"]
        return run_fixture(eval_["prompt"], fixture_dir, timeout)

    else:
        return f"Unknown eval type: {eval_type}", "error"


# ---------------------------------------------------------------------------
# Judge
# ---------------------------------------------------------------------------

def run_judge(expected_behavior: str, agent_output: str) -> tuple[int, str]:
    prompt = JUDGE_PROMPT.format(
        expected_behavior=expected_behavior,
        agent_output=agent_output[:6000],
    )
    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True, text=True, timeout=JUDGE_TIMEOUT,
        )
        output = result.stdout.strip()
        score, reason = 0, output
        for line in output.splitlines():
            if line.startswith("SCORE:"):
                try:
                    score = int(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
            elif line.startswith("REASON:"):
                reason = line.split(":", 1)[1].strip()
        return score, reason
    except subprocess.TimeoutExpired:
        return 0, "judge timed out"
    except FileNotFoundError:
        log.critical("claude CLI not found")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_result(results_dir: Path, eval_: dict, agent_output: str, agent_status: str,
                score: int, reason: str, elapsed: float) -> None:
    eval_type = eval_.get("type", "scenario")
    source = eval_.get("scenario_file", eval_.get("fixture_dir", ""))
    lines = [
        f"# Eval {eval_['id']} ({eval_type}) — {agent_status.upper()}",
        f"",
        f"**Type:** {eval_type}  |  **Source:** {source}",
        f"",
        f"**Expected behavior:** {eval_['expected_behavior']}",
        f"",
        f"**Score:** {score}/5  |  **Elapsed:** {elapsed:.1f}s",
        f"",
        f"**Judge reasoning:** {reason}",
        f"",
        f"---",
        f"",
        f"## Agent output",
        f"",
        agent_output,
    ]
    (results_dir / f"eval-{eval_['id']}.md").write_text("\n".join(lines))


def write_summary(results_dir: Path, results: list[dict], elapsed: float) -> None:
    lines = [
        "# Eval run summary",
        f"Date: {date.today().isoformat()}",
        f"Elapsed: {elapsed / 60:.1f} min",
        "",
        "| ID | Type | Score | Status |",
        "|----|------|-------|--------|",
    ]
    for r in results:
        score_str = f"{r['score']}/5" if r["score"] else "—"
        lines.append(f"| {r['id']} | {r['type']} | {score_str} | {r['status']} |")

    scored = [r for r in results if r["score"]]
    if scored:
        avg = sum(r["score"] for r in scored) / len(scored)
        lines += ["", f"**Average score:** {avg:.1f}/5  ({len(scored)} evals scored)"]

    (results_dir / "summary.md").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Eval runner for bug-bash-bot")
    parser.add_argument("--ids", default=None,
                        help="Comma-separated eval IDs to run (default: all)")
    parser.add_argument("--type", choices=["scenario", "fixture"], default=None,
                        help="Run only evals of this type")
    parser.add_argument("--no-judge", action="store_true",
                        help="Skip LLM grading — just capture agent output")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"Timeout per eval in seconds (default: {DEFAULT_TIMEOUT})")
    args = parser.parse_args()

    evals = json.loads(EVALS_FILE.read_text())["evals"]

    if args.ids:
        ids = {int(i.strip()) for i in args.ids.split(",")}
        evals = [e for e in evals if e["id"] in ids]
    if args.type:
        evals = [e for e in evals if e.get("type") == args.type]

    # Skip placeholder fixture evals
    runnable = [e for e in evals if "PLACEHOLDER" not in e.get("fixture_dir", "")]
    skipped_placeholders = len(evals) - len(runnable)
    evals = runnable

    today = date.today().isoformat()
    results_dir = RESULTS_BASE / today
    results_dir.mkdir(parents=True, exist_ok=True)

    setup_logging(results_dir)
    if skipped_placeholders:
        log.info("Skipping %d placeholder fixture evals (no real ticket recorded yet)",
                 skipped_placeholders)
    log.info("=== run_evals start  date=%s  evals=%d ===", today, len(evals))

    start = datetime.now()
    results = []

    for i, eval_ in enumerate(evals, 1):
        eval_type = eval_.get("type", "scenario")
        log.info("[%d/%d] eval-%d (%s)", i, len(evals), eval_["id"], eval_type)

        t0 = datetime.now()
        agent_output, agent_status = run_agent(eval_, args.timeout)
        elapsed = (datetime.now() - t0).total_seconds()
        log.info("  agent: %s  (%.1fs)", agent_status, elapsed)

        score, reason = 0, ""
        if not args.no_judge and agent_status not in ("timeout", "skipped"):
            score, reason = run_judge(eval_["expected_behavior"], agent_output)
            log.info("  judge: %d/5 — %s", score, reason[:120])

        save_result(results_dir, eval_, agent_output, agent_status, score, reason, elapsed)
        results.append({"id": eval_["id"], "type": eval_type,
                        "status": agent_status, "score": score})

    total_elapsed = (datetime.now() - start).total_seconds()
    write_summary(results_dir, results, total_elapsed)

    scored = [r for r in results if r["score"]]
    avg = sum(r["score"] for r in scored) / len(scored) if scored else 0
    log.info("=== done  elapsed=%.1fmin  avg_score=%.1f/5  results=%s ===",
             total_elapsed / 60, avg, results_dir)


if __name__ == "__main__":
    main()
