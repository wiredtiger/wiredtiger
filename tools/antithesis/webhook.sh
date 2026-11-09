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

export WEBHOOK_PAYLOAD=$( \
          jq -n \
            --arg tag "$tag" \
            '{
              params: (
                {
                  "antithesis.description": "docker-wt-format-test",
                  "custom.duration": "0.1",
                  "antithesis.config_image": "wt-test-format-config:\($tag)",
                  "antithesis.images": "wt-test-format:\($tag)",
                  "antithesis.report.recipients": "will.korteland@mongodb.com",
                  "custom.compose_env": "FORMAT_TEST_SCRIPT_ARGS=hello_from_compose_env"
                }
              )
            }')

          echo "Webhook payload: $WEBHOOK_PAYLOAD"
          echo $WEBHOOK_PAYLOAD |  curl --fail -u "mongodb:${antithesis_api_password}" -X POST https://mongo.antithesis.com/api/v1/launch/wiredtiger -d @-
