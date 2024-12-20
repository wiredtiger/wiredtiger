#!/bin/bash

set -ueo pipefail

VENV_PATH=".venv"
[[ -f .venv/bin/activate ]] || virtualenv -q -p python3 .venv
chmod 755 .venv/bin/activate
. "$VENV_PATH"/bin/activate

REPO_URL="https://github.com/wiredtiger/layercparse.git"
BRANCH="wtbuild-268-remove-wtdefs"
LAYERCPARSE_METADATA_FILE="$VENV_PATH/layercparse_metadata"

IS_CACHED=$(pip3 show layercparse > /dev/null 2>&1 && echo true || echo false)
if ($IS_CACHED && [[ -f "$LAYERCPARSE_METADATA_FILE" ]]); then
    LAST_TIME_CHECKED=$(grep 'last_time_checked' "$LAYERCPARSE_METADATA_FILE" | cut -d ':' -f 2 | xargs)
    CURRENT_TIME=$(date +%s)
    DIFF=$((CURRENT_TIME - LAST_TIME_CHECKED))
    if [ "$DIFF" -le 86400 ]; then
            exit 0
    fi
fi

LATEST_HASH=$(git ls-remote $REPO_URL $BRANCH | awk '{print $1}')
if [[ ! -f "$LAYERCPARSE_METADATA_FILE" || "$LATEST_HASH" != "$(grep 'hash' "$LAYERCPARSE_METADATA_FILE" | cut -d ':' -f 2 | xargs)" || $IS_CACHED == false ]]; then
    pip3 -q --disable-pip-version-check install git+"$REPO_URL@$BRANCH"
fi
echo "last_time_checked: $(date +%s)" > "$LAYERCPARSE_METADATA_FILE"
echo "hash: $LATEST_HASH" >> "$LAYERCPARSE_METADATA_FILE"
