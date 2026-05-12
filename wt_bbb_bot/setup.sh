#!/usr/bin/env bash
# Installs the devprod-mcp-proxy binary used by .mcp.json.
# Idempotent: safe to re-run.

set -euo pipefail

PROXY_BIN="${HOME}/go/bin/devprod-mcp-proxy"
GATEWAY_URL="https://app.devprod-mcp-gateway.prod.corp.mongodb.com"

echo "== wt_bbb_bot setup =="

# Verify Go
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: 'go' not found on PATH."
  echo "Install with: sudo snap install go --classic"
  exit 1
fi
echo "go: $(go version)"

# Allow Go to fetch from the private 10gen org
current=$(go env GOPRIVATE)
if [[ "${current}" != *"github.com/10gen"* ]]; then
  echo "Setting GOPRIVATE=github.com/10gen (was: '${current}')"
  go env -w GOPRIVATE=github.com/10gen
fi

# Verify gh auth so go install can clone private repos
if ! command -v gh >/dev/null 2>&1 || ! gh auth status >/dev/null 2>&1; then
  echo "ERROR: gh CLI not authenticated."
  echo "Run: sudo apt install -y gh && gh auth login"
  exit 1
fi
echo "gh: authenticated as $(gh api user --jq .login)"

# Install or upgrade the proxy
echo "Installing devprod-mcp-proxy..."
go install github.com/10gen/devprod-mcp-router/cmd/devprod-mcp-proxy@latest
echo "proxy: ${PROXY_BIN}"

# Confirm it's on PATH
if ! command -v devprod-mcp-proxy >/dev/null 2>&1; then
  echo
  echo "NOTE: devprod-mcp-proxy is at ${PROXY_BIN} but not on PATH."
  echo "Add this to your shell rc file:"
  echo "  export PATH=\"\$PATH:\$HOME/go/bin\""
  echo "Then start a new shell and re-check with: which devprod-mcp-proxy"
fi

# Verify network reachability to the gateway
if ! curl -sf -m 5 -o /dev/null "${GATEWAY_URL}/healthz" 2>/dev/null; then
  http=$(curl -sI -m 5 -o /dev/null -w "%{http_code}" "${GATEWAY_URL}/healthz" || echo "0")
  if [[ "${http}" == "302" ]]; then
    echo "gateway: reachable (302 to login — corp network OK)"
  else
    echo
    echo "WARNING: gateway not reachable (HTTP ${http})."
    echo "Check VPN / corporate network access to ${GATEWAY_URL}"
  fi
else
  echo "gateway: reachable"
fi

echo
echo "== Setup complete =="
echo
echo "Next steps (per-user, browser required):"
echo "  1. Register your Jira PAT with the gateway:"
echo "     ${GATEWAY_URL}/credentials"
echo "     (paste the same PAT you use for jira.mongodb.org)"
echo
echo "  2. Open Claude Code from this directory and ask it to fetch any"
echo "     Jira ticket. First call will trigger Okta auth — if running"
echo "     on a headless box, copy the printed URL into a laptop browser."
