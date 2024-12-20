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
        if find "$HASH_FILE" -mtime -1 | grep -q "$HASH_FILE"; then
            return 1
        fi
    fi
    return 0
}

if needs_update; then
    latest_hash=$(git ls-remote $REPO_URL $BRANCH | awk '{print $1}')
    if [[ ! -f "$HASH_FILE" || "$latest_hash" != "$(cat $HASH_FILE)" || ! is_cached ]]; then
        pip3 -q --disable-pip-version-check install git+"$REPO_URL@$BRANCH"
        echo "$latest_hash" > "$HASH_FILE"
    fi
    touch "$HASH_FILE"
fi
