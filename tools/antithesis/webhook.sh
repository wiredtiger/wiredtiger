#!/bin/sh

set -o errexit
set -o verbose

# Compute the same image tag used by build_and_push_containers.sh: mainline
# builds use the commit's git hash, patch builds use the Evergreen patch ID.
if [ "${is_patch:-}" = "true" ]; then
    tag="evg-patch-${version_id}"
else
    tag="${revision}"
fi

if [ -n "${antithesis_image_tag:-}" ]; then
    echo "Using provided tag: '$antithesis_image_tag'"
    tag=$antithesis_image_tag
fi

# Route the report based on the build type (mirrors the SLS convention in
# ds-services/storage/etc/evergreen_antithesis.yml):
#   - patch builds: email the patch author only
#   - mainline builds: post to the team's Slack channel
if [ "${is_patch:-}" = "true" ]; then
    recipient="${author_email}"
    send_slack="false"
    # Mark patch runs ephemeral so their findings aren't treated as regressions
    # (see https://antithesis.com/docs/reports/findings/).
    ephemeral="true"
    description="wt-format-test - Patch: ${version_id} submitted by ${author}"
else
    # TODO: switch to a Slack notification once an incoming webhook is set up for
    # the team channel (see below). For now, email the team directly.
    recipient="will.korteland@mongodb.com,mikeo@mongodb.com"
    send_slack="false"
    ephemeral="false"
    description="wt-format-test - Commit: ${revision} (${version_id})"
fi

# NOTE: to enable Slack notifications for mainline runs, create a Slack incoming
# webhook for the desired channel, then set the callback URL below, define the
# 'antithesis_slack_hook_token' Evergreen project variable with the webhook token,
# and set send_slack="true" in the non-patch branch above.
slack_callback_url="https://hooks.slack.com/services/REPLACE_ME/REPLACE_ME"

# Note: antithesis.duration is the standard webhook parameter and is in MINUTES
# (unlike the SLS endpoint's custom.duration, which is in hours). Settable via the
# antithesis_duration expansion in test/evergreen.yml (ubuntu2004-antithesis variant).
duration="${antithesis_duration:-60}"

export WEBHOOK_PAYLOAD=$( \
          jq -n \
            --arg tag "$tag" \
            --arg duration "$duration" \
            --arg description "$description" \
            --arg recipient "$recipient" \
            --arg send_slack "$send_slack" \
            --arg slack_callback_url "$slack_callback_url" \
            --arg slack_token "${antithesis_slack_hook_token:-}" \
            --arg ephemeral "$ephemeral" \
            --arg source "${task_name:-wiredtiger-format}" \
            --arg branch "${branch_name:-}" \
            --arg evg_version "${version_id:-}" \
            --arg evg_variant "${build_variant:-}" \
            --arg evg_url "https://spruce.mongodb.com/version/${version_id}" \
            '{
              params: (
                {
                  "antithesis.description": $description,
                  "antithesis.duration": $duration,
                  "antithesis.config_image": "wt-test-format-config:\($tag)",
                  "antithesis.images": "wt-test-format:\($tag)",
                  "antithesis.report.recipients": $recipient,
                  "antithesis.is_ephemeral": $ephemeral,
                  "antithesis.source": $source,
                  "custom.test_suite": "format",
                  "custom.evergreen_branch": $branch,
                  "custom.evergreen_version": $evg_version,
                  "custom.evergreen_build_variant": $evg_variant,
                  "custom.evergreen_url": $evg_url,
                  "custom.compose_env": "FORMAT_TEST_SCRIPT_ARGS=hello_from_compose_env"
                }
                | if $send_slack == "false"
                  then .
                  else . + {
                    "antithesis.integrations.slack.callback_url": $slack_callback_url,
                    "antithesis.integrations.slack.token": $slack_token,
                  }
                  end
              )
            }')

          echo "Webhook payload: $WEBHOOK_PAYLOAD"

          # The launch endpoint rate-limits (HTTP 429); retry with a pause between attempts.
          max_attempts=20
          for attempt in $(seq 1 $max_attempts); do
              http_code=$(echo $WEBHOOK_PAYLOAD | curl -sS -o /tmp/webhook_response.txt -w "%{http_code}" \
                  -u "mongodb:${antithesis_api_password}" \
                  -X POST https://mongo.antithesis.com/api/v1/launch/wiredtiger -d @-) || true
              if [ "$http_code" != "429" ]; then
                  break
              fi
              echo "Launch returned HTTP 429 (attempt $attempt/$max_attempts), retrying in 5s..."
              sleep 5
          done

          cat /tmp/webhook_response.txt
          case "$http_code" in
              2*) ;;
              *) echo "Webhook launch failed with HTTP status '$http_code'"; exit 1 ;;
          esac
