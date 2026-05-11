# Workflow Reference

## Primary rule

Start every new investigation in triage mode (`@paths/investigate.md` Phase 0) unless the
user explicitly asks for storage inspection or Jira-only work.

## Escalation order

1. **Investigate** (`@paths/investigate.md`) — always start here for unclear failures
2. **Storage inspection** (`@paths/wt-cli.md`) — if the issue points to persisted state,
   files, metadata, or logs
3. **Jira update** (`@paths/jira.md`) — once there is something worth recording

## Good defaults

- Prefer root-cause investigation over quick fixes
- Prefer read-only `wt` inspection — never run destructive commands without confirmation
- Keep Jira comments concise and evidence-backed
- Do not post to Jira until the working theory is clear
