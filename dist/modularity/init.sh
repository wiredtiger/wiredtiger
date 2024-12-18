#!/bin/bash

set -ueo pipefail

[[ -f .venv/bin/activate ]] || virtualenv -q -p python3 .venv
chmod 755 .venv/bin/activate

. .venv/bin/activate

REPO_URL="https://github.com/wiredtiger/layercparse.git"
BRANCH="main"
HASH_FILE=".layercparse_commit_hash"
LATEST_HASH=$(git ls-remote "$REPO_URL" "$BRANCH" | awk '{print $1}')

if [[ ! -f "$HASH_FILE" || "$LATEST_HASH" != "$(cat $HASH_FILE)" ]]; then
    pip3 -q --disable-pip-version-check install git+"$REPO_URL@$BRANCH"
    echo "$LATEST_HASH" > "$HASH_FILE"
fi
