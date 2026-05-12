#!/usr/bin/env python3
"""
mock_mcp_server.py — Stdio MCP server that replays recorded fixture responses.

Loaded by claude -p via --mcp-config when running fixture-type evals.
Reads fixture calls from <fixture_dir>/calls.jsonl and matches by tool + args.

Usage (via --mcp-config, not invoked directly):
    python testing/mock_mcp_server.py --fixture-dir testing/evals/fixtures/BF-12345
"""

import argparse
import json
import sys
from pathlib import Path

PROTOCOL_VERSION = "2024-11-05"
SERVER_INFO = {"name": "mock-devprod-mcp-gateway", "version": "1.0.0"}

# Tool declarations — subset used by bug-bash-bot. Schemas are intentionally
# minimal (just enough for the MCP handshake; the bot uses them by name).
TOOLS = [
    {"name": n, "description": d, "inputSchema": {"type": "object", "properties": {}}}
    for n, d in [
        ("jira_get_issue",              "Get a Jira issue by key"),
        ("jira_get_issue_comments",     "Get comments on a Jira issue"),
        ("jira_search_issues",          "Search Jira issues with JQL"),
        ("jira_add_comment",            "Add a comment to a Jira issue"),
        ("bb_get_bf",                   "Get Build Baron failure details"),
        ("bb_get_bfg",                  "Get a Build Baron failure group"),
        ("bb_get_bfg_by_task",          "Get failure group for an Evergreen task"),
        ("bb_search_bfgs",              "Search Build Baron failure groups"),
        ("evg_get_task_log_summary",    "Summarize Evergreen task logs"),
        ("evg_get_raw_task_logs",       "Fetch raw Evergreen task logs"),
        ("evg_get_test_results_summary","Summarize Evergreen test results"),
        ("evg_get_test_results_detailed","Get detailed Evergreen test results"),
        ("evg_get_patch_failed_jobs",   "Get failed jobs for an Evergreen patch"),
        ("git_log",                     "Walk commit history"),
        ("git_blame",                   "Blame a file or function"),
        ("git_search",                  "Search commits by message or SHA"),
        ("git_diff",                    "Diff between two commits"),
        ("git_show",                    "Inspect a single commit"),
    ]
]


def load_calls(fixture_dir: Path) -> list[dict]:
    calls_file = fixture_dir / "calls.jsonl"
    if not calls_file.exists():
        return []
    return [
        json.loads(line)
        for line in calls_file.read_text().splitlines()
        if line.strip()
    ]


def find_response(calls: list[dict], tool: str, args: dict) -> dict | None:
    """Match by tool name + args. Falls back to tool-name-only match."""
    for call in calls:
        if call["tool"] == tool and call.get("args") == args:
            return call["response"]
    for call in calls:
        if call["tool"] == tool:
            return call["response"]
    return None


def respond(req_id, result: dict) -> None:
    msg = json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result})
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def respond_error(req_id, code: int, message: str) -> None:
    msg = json.dumps({"jsonrpc": "2.0", "id": req_id,
                      "error": {"code": code, "message": message}})
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-dir", required=True,
                        help="Directory containing calls.jsonl")
    args = parser.parse_args()

    fixture_dir = Path(args.fixture_dir)
    calls = load_calls(fixture_dir)

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            req = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        req_id = req.get("id")
        method = req.get("method", "")

        if method == "initialize":
            respond(req_id, {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": SERVER_INFO,
            })

        elif method == "notifications/initialized":
            pass  # notification — no response

        elif method == "tools/list":
            respond(req_id, {"tools": TOOLS})

        elif method == "tools/call":
            tool_name = req.get("params", {}).get("name", "")
            tool_args = req.get("params", {}).get("arguments", {})
            response = find_response(calls, tool_name, tool_args)
            if response is None:
                content = [{"type": "text", "text": json.dumps({
                    "error": f"No fixture recorded for tool '{tool_name}' with args {tool_args}"
                })}]
            else:
                content = [{"type": "text", "text": json.dumps(response)}]
            respond(req_id, {"content": content})

        else:
            # Unknown method — return empty result
            respond(req_id, {})


if __name__ == "__main__":
    main()
