#!/usr/bin/env python3
"""
WiredTiger BBB Bot — batch / automation entry point.

For interactive use, run the skill directly in Claude Code:
    /bug-bash-bot BF-XXXXX

This script is for programmatic/cron use: it loads SKILL.md as the system
prompt and drives the same analysis via the Anthropic SDK.

Usage:
    python main.py BF-XXXXX
    python main.py "Triage BF-12345, BF-12346, BF-12347"

Environment:
    ANTHROPIC_API_KEY    — Claude API key (request via Grove)
    DEVPROD_MCP_URL      — devprod-mcp-gateway base URL
    DEVPROD_MCP_TOKEN    — devprod-mcp-gateway auth token
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

import anthropic

SKILL = Path(__file__).parent / "SKILL.md"
MODEL = "claude-opus-4-7"
MAX_TOKENS = 8096

# Tools declared in SKILL.md — must stay in sync with the MCP tool list there.
TOOLS = [
    {
        "name": "jira_get_issue",
        "description": "Get a Jira issue by key (e.g. BF-12345)",
        "input_schema": {
            "type": "object",
            "properties": {"issue_key": {"type": "string"}},
            "required": ["issue_key"],
        },
    },
    {
        "name": "jira_get_issue_comments",
        "description": "Get comments on a Jira issue",
        "input_schema": {
            "type": "object",
            "properties": {"issue_key": {"type": "string"}},
            "required": ["issue_key"],
        },
    },
    {
        "name": "jira_search_issues",
        "description": "Search Jira issues with a JQL query",
        "input_schema": {
            "type": "object",
            "properties": {
                "jql": {"type": "string"},
                "max_results": {"type": "integer"},
            },
            "required": ["jql"],
        },
    },
    {
        "name": "jira_add_comment",
        "description": "Add a comment to a Jira issue",
        "input_schema": {
            "type": "object",
            "properties": {
                "issue_key": {"type": "string"},
                "comment": {"type": "string"},
            },
            "required": ["issue_key", "comment"],
        },
    },
    {
        "name": "bb_get_bf",
        "description": "Get build failure details from Build Baron",
        "input_schema": {
            "type": "object",
            "properties": {"bf_id": {"type": "string"}},
            "required": ["bf_id"],
        },
    },
    {
        "name": "bb_get_bfg",
        "description": "Get a build failure group from Build Baron",
        "input_schema": {
            "type": "object",
            "properties": {"bfg_id": {"type": "string"}},
            "required": ["bfg_id"],
        },
    },
    {
        "name": "bb_get_bfg_by_task",
        "description": "Get the build failure group for an Evergreen task",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "bb_search_bfgs",
        "description": "Search build failure groups in Build Baron",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    {
        "name": "evg_get_task_log_summary",
        "description": "Summarize logs for an Evergreen task",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "evg_get_raw_task_logs",
        "description": "Fetch raw logs for an Evergreen task",
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "log_type": {"type": "string", "description": "task, agent, or system"},
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "evg_get_test_results_summary",
        "description": "Get a summary of test results for an Evergreen task",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "evg_get_test_results_detailed",
        "description": "Get detailed test results with raw output for an Evergreen task",
        "input_schema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
        },
    },
    {
        "name": "evg_get_patch_failed_jobs",
        "description": "Get all failed jobs for an Evergreen patch",
        "input_schema": {
            "type": "object",
            "properties": {"patch_id": {"type": "string"}},
            "required": ["patch_id"],
        },
    },
]


def run(task: str) -> None:
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": task}]
    system = SKILL.read_text()

    while True:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=system,
            messages=messages,
            tools=TOOLS,
        )

        for block in response.content:
            if hasattr(block, "text"):
                print(block.text, end="", flush=True)

        if response.stop_reason != "tool_use":
            print()
            break

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            print(f"\n[tool: {block.name}]", flush=True)
            result = call_mcp(block.name, block.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(result),
            })

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})


def call_mcp(tool: str, params: dict) -> dict:
    url = os.environ.get("DEVPROD_MCP_URL", "")
    token = os.environ.get("DEVPROD_MCP_TOKEN", "")
    if not url:
        return {"error": "DEVPROD_MCP_URL not set — configure the gateway first"}

    payload = json.dumps({"tool": tool, "params": params}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py BF-XXXXX")
        print("       python main.py 'Triage BF-12345, BF-12346'")
        sys.exit(1)

    task = " ".join(sys.argv[1:])
    print(f"[wt-bbb-bot] {task}\n")
    run(task)
