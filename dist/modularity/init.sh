#!/bin/bash

set -ueo pipefail

[[ -f .venv/bin/activate ]] || virtualenv -q -p python3 .venv
chmod 755 .venv/bin/activate
. .venv/bin/activate

REPO_URL="https://github.com/wiredtiger/layercparse.git"
BRANCH="main"
HASH_FILE=".venv/layercparse_hash"

is_cached() {
  pip3 -qq show layercparse > /dev/null 2>&1
}

needs_update() {
    if is_cached && [[ -f "$HASH_FILE" ]]; then
        LAST_TIME_CHECKED=$(stat -c %Y "$HASH_FILE")
        CURRENT_TIME=$(date +%s)
        DIFF=$((CURRENT_TIME - LAST_TIME_CHECKED))
        if [[ "$DIFF" -le 86400 ]]; then
            return 1
        fi
    fi
    return 0
}

if needs_update; then
    LATEST_HASH=$(git ls-remote $REPO_URL $BRANCH | awk '{print $1}')
    if [[ ! -f "$HASH_FILE" || "$LATEST_HASH" != "$(cat $HASH_FILE)" || ! is_cached ]]; then
        pip3 -q --disable-pip-version-check install git+"$REPO_URL@$BRANCH"
        echo "$LATEST_HASH" > "$HASH_FILE"
    fi
    touch "$HASH_FILE"
fi
