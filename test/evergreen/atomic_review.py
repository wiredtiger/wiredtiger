#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

import json
import os
import re
import sys
from urllib.request import Request, urlopen

TEAM = 'storage-engines-atomics-reviewers'
HEADERS = {'src/include/gcc.h', 'src/include/msvc.h', 'src/include/hardware.h'}
TOKENS = re.compile(r'\b(?:__wt_atomic\w*|__wt_tsan\w*|__tsan_\w*|WT_\w*BARRIER\w*|'
    r'WT_READ_ONCE|WT_WRITE_ONCE|__atomic_\w*|__sync_\w*|__ATOMIC_\w*|TSAN_BUILD)\b|'
    r'no_sanitize[^\n]*thread')


def needs_review(files):
    return any(file['filename'] in HEADERS or file.get('previous_filename') in HEADERS or
        TOKENS.search(file.get('patch', '')) for file in files)


def api(url, method='GET', body=None):
    headers = {'Authorization': f"Bearer {os.environ['GITHUB_TOKEN']}",
        'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json'}
    data = json.dumps(body).encode() if body is not None else None

    with urlopen(Request(url, data=data, headers=headers, method=method), timeout=30) as response:
        if response.status != {'GET': 200, 'POST': 201, 'DELETE': 200}[method]:
            raise ValueError(f'Unexpected HTTP status: {response.status}')

        return json.load(response), response.headers.get('Link', '')


def run(number):
    url = f'https://api.github.com/repos/wiredtiger/wiredtiger/pulls/{int(number)}'
    pr, _ = api(url)
    if pr['state'] == 'closed':
        return
    if pr['state'] != 'open':
        raise ValueError(f"Unexpected PR state: {pr['state']}")

    files = []
    next_page = f'{url}/files?per_page=100'
    while next_page:
        page, links = api(next_page)
        if not isinstance(page, list):
            raise ValueError('Expected a list of changed files')
        files.extend(page)

        next_link = re.search(r'<([^>]+)>;\s*rel="next"', links)
        next_page = next_link.group(1) if next_link else None

    needed = needs_review(files)
    requested = any(team['slug'] == TEAM for team in pr['requested_teams'])
    print(f'Expert review needed: {needed}; already requested: {requested}')

    if needed != requested:
        method = 'POST' if needed else 'DELETE'
        pr, _ = api(f'{url}/requested_reviewers', method,
            body={'reviewers': [], 'team_reviewers': [TEAM]})

        if any(team['slug'] == TEAM for team in pr['requested_teams']) != needed:
            raise ValueError('GitHub did not apply the expected review request')

        print(f'{method}: review request for @wiredtiger/{TEAM}')


if __name__ == '__main__':
    run(sys.argv[1])
