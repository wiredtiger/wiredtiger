#!/usr/bin/env python3
# Copyright (c) 2026 MongoDB, Inc.
# See the file LICENSE for redistribution information.

import json
import re
import subprocess
import sys

TEAM = 'storage-engines-atomics-reviewers'
HEADERS = {'src/include/gcc.h', 'src/include/msvc.h', 'src/include/hardware.h'}
TOKENS = re.compile(r'\b(?:__wt_atomic\w*|__wt_tsan\w*|__tsan_\w*|WT_\w*BARRIER\w*|'
    r'WT_READ_ONCE|WT_WRITE_ONCE|__atomic_\w*|__sync_\w*|__ATOMIC_\w*|TSAN_BUILD)\b|'
    r'no_sanitize[^\n]*thread')

def needs_review(files):
    return any(file['filename'] in HEADERS or file.get('previous_filename') in HEADERS or
        TOKENS.search(file.get('patch', '')) for file in files)

def api(path, *options, body=None):
    # gh inherits GH_TOKEN; never pass credentials as command-line arguments.
    return json.loads(subprocess.check_output(['gh', 'api', path, *options],
        input=json.dumps(body) if body is not None else None, text=True))

def run(repo, number):
    path = f'repos/{repo}/pulls/{int(number)}'
    pr = api(path)
    if pr['state'] != 'open':
        return
    pages = api(f'{path}/files?per_page=100', '--paginate', '--slurp')
    needed = needs_review([file for page in pages for file in page])
    requested = any(team['slug'] == TEAM for team in pr['requested_teams'])
    print(f'Expert review needed: {needed}; already requested: {requested}')
    if needed != requested:
        method = 'POST' if needed else 'DELETE'
        api(f'{path}/requested_reviewers', '--method', method, '--input', '-',
            body={'reviewers': [], 'team_reviewers': [TEAM]})
        print(f'{method}: review request for @wiredtiger/{TEAM}')

if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
