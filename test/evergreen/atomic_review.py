#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

import os
import re
import sys

import requests

TEAM = 'storage-engines-atomics-reviewers'
HEADERS = {'src/include/gcc.h', 'src/include/msvc.h', 'src/include/hardware.h'}
TOKENS = re.compile(r'\b(?:__wt_atomic\w*|__wt_tsan\w*|__tsan_\w*|WT_\w*BARRIER\w*|'
    r'WT_READ_ONCE|WT_WRITE_ONCE|__atomic_\w*|__sync_\w*|__ATOMIC_\w*|TSAN_BUILD)\b|'
    r'no_sanitize[^\n]*thread')

def needs_review(files):
    return any(file['filename'] in HEADERS or file.get('previous_filename') in HEADERS or
        TOKENS.search(file.get('patch', '')) for file in files)

def api(url, method='GET', body=None):
    headers = {'Authorization': f"token {os.environ['GITHUB_TOKEN']}",
        'Accept': 'application/vnd.github.v3+json'}
    response = requests.request(method, url, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    return response

def run(repo, number):
    url = f'https://api.github.com/repos/{repo}/pulls/{int(number)}'
    pr = api(url).json()
    if pr['state'] != 'open':
        return
    files = []
    next_page = f'{url}/files?per_page=100'
    while next_page:
        response = api(next_page)
        files.extend(response.json())
        next_page = response.links.get('next', {}).get('url')
    needed = needs_review(files)
    requested = any(team['slug'] == TEAM for team in pr['requested_teams'])
    print(f'Expert review needed: {needed}; already requested: {requested}')
    if needed != requested:
        method = 'POST' if needed else 'DELETE'
        api(f'{url}/requested_reviewers', method,
            body={'reviewers': [], 'team_reviewers': [TEAM]})
        print(f'{method}: review request for @wiredtiger/{TEAM}')

if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
