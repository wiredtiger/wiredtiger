#!/bin/bash

set -ueo pipefail

VENV_PATH=".venv"
[[ -f .venv/bin/activate ]] || virtualenv -q -p python3 .venv
chmod 755 .venv/bin/activate
. "$VENV_PATH"/bin/activate

REPO_URL="https://github.com/wiredtiger/layercparse.git"
BRANCH="main"
IS_CACHED=$(pip3 show layercparse > /dev/null 2>&1 && echo true || echo false)
CACHE_TIMEOUT=0.5
LATEST_HASH_CMD="git ls-remote \"$REPO_URL\" \"$BRANCH\" | awk '{print \$1}'"
if ($IS_CACHED); then
    if ! LATEST_HASH=$(timeout "$CACHE_TIMEOUT"s bash -c "$LATEST_HASH_CMD"); then
        echo "Fetching LATEST_HASH timed out. Using cached layercparse version."
        exit 0
    fi
else
LATEST_HASH=$(bash -c "$LATEST_HASH_CMD")
fi

HASH_FILE="$VENV_PATH/layercparse_commit_hash"
if [[ ! -f "$HASH_FILE" || "$LATEST_HASH" != "$(cat $HASH_FILE)" || $IS_CACHED == false ]]; then
    pip3 -q --disable-pip-version-check install git+"$REPO_URL@$BRANCH"
    echo "$LATEST_HASH" > "$HASH_FILE"
fi