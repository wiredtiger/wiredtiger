#!/usr/bin/env python3
"""
record_fixtures.py — Record MCP responses for a BF/WT ticket into fixture files.

Fetches each tool's response individually via `claude -p --allowedTools <tool>`
and saves them to testing/evals/fixtures/<ticket>/calls.jsonl.

Usage:
    python testing/record_fixtures.py WT-16620
    python testing/record_fixtures.py BF-12345 --overwrite
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
FIXTURES_BASE = REPO_ROOT / "testing" / "evals" / "fixtures"
FETCH_TIMEOUT = 60


def claude_fetch(tool: str, args: dict) -> dict | None:
    """Ask claude -p to call one MCP tool and return the raw JSON response."""
    args_str = json.dumps(args)
    prompt = (
        f"Call the tool {tool} with these arguments: {args_str}\n"
        f"Reply with ONLY the raw JSON response from the tool. "
        f"No explanation, no markdown formatting, just the JSON object."
    )
    try:
        result = subprocess.run(
            ["claude", "-p", prompt, "--allowedTools", f"mcp__devprod-mcp-gateway__{tool}"],
            capture_output=True, text=True, timeout=FETCH_TIMEOUT,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  ERROR fetching {tool}: {e}")
        return None

    output = result.stdout.strip()
    # Extract first JSON object or array from output
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        try:
            start = output.index(start_char)
            end = output.rindex(end_char) + 1
            return json.loads(output[start:end])
        except (ValueError, json.JSONDecodeError):
            continue

    print(f"  WARNING: could not parse JSON from {tool} response:\n  {output[:300]}")
    return None


def record(ticket: str, overwrite: bool) -> None:
    fixture_dir = FIXTURES_BASE / ticket
    calls_file = fixture_dir / "calls.jsonl"

    if calls_file.exists() and not overwrite:
        sys.exit(f"ERROR: {calls_file} already exists. Use --overwrite to replace it.")

    fixture_dir.mkdir(parents=True, exist_ok=True)
    calls = []

    def fetch_and_record(tool: str, args: dict) -> dict | None:
        print(f"  Fetching {tool}({list(args.keys())})...")
        response = claude_fetch(tool, args)
        if response is not None:
            calls.append({"tool": tool, "args": args, "response": response})
            print(f"    OK ({len(json.dumps(response))} chars)")
        else:
            print(f"    SKIPPED (no response)")
        return response

    print(f"\n[1/4] Fetching Jira ticket: {ticket}")
    issue = fetch_and_record("jira_get_issue", {"issue_key": ticket})
    fetch_and_record("jira_get_issue_comments", {"issue_key": ticket})

    # Extract task IDs from the Jira issue to fetch Evergreen + Build Baron data
    task_ids = []
    if issue:
        # Look for task IDs in custom fields or description
        desc = issue.get("description", "") or ""
        failing_tasks = issue.get("Failing Tasks", "") or issue.get("failing_tasks", "") or ""
        # Try to extract Evergreen task IDs (format: mongodb_<project>_<hash>)
        import re
        task_pattern = r'[a-f0-9]{24,}'  # Evergreen task IDs are 24+ hex chars
        candidates = re.findall(task_pattern, desc + " " + str(failing_tasks))
        task_ids = list(dict.fromkeys(candidates))[:2]  # dedupe, take first 2

    if task_ids:
        print(f"\n[2/4] Fetching Build Baron data for task(s): {task_ids}")
        for task_id in task_ids:
            fetch_and_record("bb_get_bfg_by_task", {"task_id": task_id})

        print(f"\n[3/4] Fetching Evergreen log summary")
        for task_id in task_ids:
            fetch_and_record("evg_get_task_log_summary", {"task_id": task_id})

        print(f"\n[4/4] Fetching raw task logs")
        for task_id in task_ids[:1]:  # raw logs can be large — just first task
            fetch_and_record("evg_get_raw_task_logs", {"task_id": task_id, "log_type": "task"})
    else:
        print(f"\n[2-4/4] Skipping Evergreen/Build Baron — no task IDs found in ticket")
        print("  You can add them manually to calls.jsonl if needed.")

    # Write fixture file
    lines = [json.dumps(call) for call in calls]
    calls_file.write_text("\n".join(lines) + "\n")

    print(f"\nRecorded {len(calls)} tool calls → {calls_file}")
    print("\nCalls recorded:")
    for call in calls:
        print(f"  {call['tool']}({list(call['args'].keys())})")

    if len(calls) < 2:
        print("\nWARNING: few calls recorded — devprod-mcp-gateway may not be connected.")
        print("Connect it in Claude Code settings and re-run with --overwrite.")

    print(f"\nNext: add a fixture eval to evals.json:")
    print(f'  "type": "fixture", "fixture_dir": "{ticket}", "prompt": "{ticket}"')


def main() -> None:
    parser = argparse.ArgumentParser(description="Record MCP fixtures for a ticket")
    parser.add_argument("ticket", help="Ticket key (e.g. WT-16620 or BF-12345)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing fixture file")
    args = parser.parse_args()

    print(f"Recording fixtures for {args.ticket.upper()}")
    print("(Requires devprod-mcp-gateway connected in Claude Code)\n")
    record(args.ticket.upper(), args.overwrite)


if __name__ == "__main__":
    main()
