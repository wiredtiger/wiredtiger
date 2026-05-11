# WiredTiger BBB Bot

AI-powered build failure triage for WiredTiger, backed by the Claude SDK and devprod-mcp-gateway.

Modeled after [bf-analysis](https://github.com/10gen/performance-ai/tree/main/dsi-skills/skills/bf-analysis)
from the performance-ai repo, adapted for WT correctness and test failures.

## Architecture

```
SKILL.md  ←  /wt-analyze BF-XXXXX  (interactive, in Claude Code)
  └── paths/priority.md       — rank multiple open BFs
  └── paths/investigate.md    — deep-dive root cause analysis
  └── paths/build.md          — reproduce locally, propose fix
  └── paths/wt-cli.md         — inspect WT data directories
  └── paths/repro-format.md   — test/format repro workflow

main.py   ←  python main.py BF-XXXXX  (batch / cron automation)
  └── loads SKILL.md as system prompt → Anthropic SDK → devprod-mcp-gateway
```

## Setup (one-time, per developer)

1. **Install the devprod-mcp-proxy** (the bridge to Jira / Evergreen / Build Baron tools):

   ```bash
   ./scripts/setup.sh
   ```

   Requires Go, `gh` CLI authenticated, and corp-network access to `*.corp.mongodb.com`.
   Installs `~/go/bin/devprod-mcp-proxy` and verifies the gateway is reachable.

2. **Register your Jira PAT with the gateway** (per-user, one-time):

   Open in a browser: <https://app.devprod-mcp-gateway.prod.corp.mongodb.com/credentials>
   Paste the PAT you use for `jira.mongodb.org`. CorpSecure handles auth.

3. **Open Claude Code from this directory.** The checked-in `.mcp.json` is loaded
   automatically. The first MCP call triggers Okta auth — on a headless box, copy the
   printed authorization URL into a laptop browser to complete the flow.

## Interactive use (recommended)

Install the skill into Claude Code and invoke it with a ticket key:

```bash
# Link skill into Claude Code's skills directory
ln -sf "$(pwd)/wt_bbb_bot" ~/.claude/skills/wt-analyze

# Then in Claude Code:
/wt-analyze BF-12345
```

## Batch / automation use

```bash
pip install -r requirements.txt

# Credentials (request via Grove)
export ANTHROPIC_API_KEY=<your-key>
export DEVPROD_MCP_URL=<gateway-url>
export DEVPROD_MCP_TOKEN=<gateway-token>

python main.py BF-12345
python main.py "Triage BF-12345, BF-12346, BF-12347"
```

## File layout

| File | Purpose |
|------|---------|
| `SKILL.md` | Entry point and 7-step triage process |
| `paths/priority.md` | Rank and score multiple open BFs |
| `paths/investigate.md` | Deep-dive: logs, stack traces, subsystem mapping |
| `paths/build.md` | Local reproduction and fix verification |
| `paths/wt-cli.md` | `wt` CLI for inspecting WT databases |
| `paths/repro-format.md` | Reproducing `test/format` failures |
| `reference/safety-rules.md` | Safety constraints for WT data operations |
| `reference/output-template.md` | Structured output format for all investigations |
| `templates/bf-comment.md` | Jira comment template (Jira wiki markup) |
| `scripts/repro_format_tmux.sh` | tmux-based format repro helper |
| `scripts/setup.sh` | One-time per-developer setup (installs devprod-mcp-proxy) |
| `.mcp.json` | MCP server config — auto-loaded by Claude Code from this directory |
| `main.py` | Batch launcher (loads SKILL.md via Anthropic SDK) |
| `testing/evals/evals.json` | Evals for skill correctness |
