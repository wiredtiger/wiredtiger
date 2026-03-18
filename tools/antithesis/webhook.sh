export WEBHOOK_PAYLOAD=$( \
          jq -n \
            '{
              params: (
                {
                  "antithesis.description": "docker-wt-format-test",
                  "custom.duration": "0.1",
                  "antithesis.config_image": "wt-test-format-config:debug-have-diagnostic-willk",
                  "antithesis.images": "wt-test-format:debug-have-diagnostic-willk",
                  "antithesis.report.recipients": "will.korteland@mongodb.com",
                  "custom.compose_env": "poop!!!!!!!"
                }
              )
            }')

          echo "Webhook payload: $WEBHOOK_PAYLOAD"
          echo $WEBHOOK_PAYLOAD |  curl --fail -u "mongodb:${antithesis_api_password}" -X POST https://mongo.antithesis.com/api/v1/launch/wiredtiger -d @-
