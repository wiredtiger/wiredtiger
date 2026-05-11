# WiredTiger BBB Bot

AI-powered build failure triage for WiredTiger, backed by the Claude SDK and devprod-mcp-gateway.

Modeled after [bf-analysis](https://github.com/10gen/performance-ai/tree/main/dsi-skills/skills/bf-analysis)
from the performance-ai repo, adapted for WT correctness and test failures.

## Architecture

```
SKILL.md  ←  /bug-bash-bot BF-XXXXX  (interactive, in Claude Code)
  │
  ├── paths/                        BF-specific triage workflows
  │     ├── investigate.md          gather evidence → delegate to systematic-debugging
  │     ├── priority.md             rank and score multiple open BFs
  │     └── build.md                build WT + extract repro → delegate to wiredtiger-test-format
  │
  ├── skills/                       reusable, independently invokable skills
  │     ├── jira/                   read tickets, search, post comments
  │     ├── github/                 commit investigation, blame, diff
  │     ├── wt-cli/                 inspect WT data directories and WAL
  │     ├── wiredtiger-test-format/ run test/format, tracing, parallel repro
  │     ├── systematic-debugging/   root cause methodology (before any fix)
  │     ├── disagg-page-inspection/ inspect WT pages in SLS disagg storage
  │     │     └── references/       setup, navigation, decoding, decryption, grpc
  │     └── help-ticket-triage/     triage HELP tickets with FTDC data
  │
  ├── reference/
  │     ├── workflow.md             escalation order and good defaults
  │     ├── output-template.md      structured output for all investigations
  │     └── safety-rules.md        safety constraints for WT data operations
  │
  ├── templates/
  │     └── bf-comment.md          Jira wiki markup comment template
  │
  ├── scripts/
  │     └── repro_format_tmux.sh   tmux-based format repro helper
  │
  └── main.py                       batch / automation entry point (Anthropic SDK)
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

## Architecture principle

**`paths/`** = BF-specific context and evidence gathering steps, routed to from `SKILL.md`.  
**`skills/`** = reusable, service-specific or methodology skills — independently invokable and referenced by paths.

## Interactive use (recommended)

Install the skill into Claude Code and invoke it with a ticket key:

```bash
ln -sf "$(pwd)/wt_bbb_bot" ~/.claude/skills/bug-bash-bot
```

Then in Claude Code:
```
/bug-bash-bot BF-12345
```

No credentials needed — Claude Code handles the devprod-mcp-gateway connection natively.

## Batch / automation use

```bash
pip install -r requirements.txt

export ANTHROPIC_API_KEY=<your-key>       # request via Grove
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

## What's implemented

| Component | Status |
|---|---|
| `SKILL.md` — 7-step triage process + routing | Done |
| `paths/investigate.md` | Done |
| `paths/priority.md` | Done |
| `paths/build.md` | Done |
| `skills/jira/` | Done |
| `skills/github/` | Done |
| `skills/wt-cli/` | Done |
| `skills/wiredtiger-test-format/` | Done |
| `skills/systematic-debugging/` | Done |
| `skills/disagg-page-inspection/` + all references | Done |
| `skills/help-ticket-triage/` | Done |
| `main.py` SDK launcher | Scaffolded — needs MCP gateway wiring |
| Evals runner | Not yet — `testing/evals/evals.json` has 9 cases ready |
