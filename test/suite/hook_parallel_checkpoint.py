#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# [TEST_TAGS]
# ignored_file
# [END_TAGS]

# hook_parallel_checkpoint.py
#
# Enable parallel checkpoints for all Python unit tests by appending a
# checkpoint_threads=... configuration to wiredtiger_open().
#
# Usage examples:
#   ../test/suite/run.py --hook parallel_checkpoint
#   ../test/suite/run.py --hook "parallel_checkpoint=(threads=8)"
#
# The hook will not override tests with explicit checkpoint_threads= in the
# connection configuration string.

from __future__ import print_function

import re, wthooks
from wttest import WiredTigerTestCase

def strip_matching_parens(s):
    if len(s) >= 2 and s[0] == '(' and s[-1] == ')':
        return s[1:-1]
    return s

def config_split(config):
    pos = config.find('=')
    if pos >= 0:
        left = config[:pos]
        right = config[pos + 1:]
    else:
        left = config
        right = ''
    return left, strip_matching_parens(right)

def parse_hook_args(arg):
    """
    Parse the hook argument string into a dict of key->value.

    Accepts:
      --hook parallel_checkpoint
      --hook parallel_checkpoint=8
      --hook "parallel_checkpoint=(threads=8)"
    """
    params = {}

    if not arg:
        return params

    arg = strip_matching_parens(str(arg).strip())
    if not arg:
        return params

    if re.fullmatch(r'[0-9]+', arg):
        params['threads'] = arg
        return params

    config_list = re.split(r",(?=(?:[^(]*[(][^)]*[)])*[^)]*$)", arg)
    for cfg in config_list:
        cfg = cfg.strip()
        if not cfg:
            continue
        key, val = config_split(cfg)
        if not key:
            continue
        params[key] = val

    return params

def wiredtiger_open_replace(orig_wiredtiger_open, homedir, conn_config, threads):
    """
    HOOK_REPLACE implementation for wiredtiger_open.

    If the config already sets checkpoint_threads=..., leave it alone.
    Otherwise append ",checkpoint_threads=<threads>".
    """
    if conn_config is None:
        conn_config = ''

    # Don't override tests that explicitly set checkpoint_threads.
    if 'checkpoint_threads=' in conn_config:
        WiredTigerTestCase.verbose(
            None, 3,
            'parallel_checkpoint hook: existing checkpoint_threads config found, '
            'leaving configuration unchanged')
        return orig_wiredtiger_open(homedir, conn_config)

    extra = ',checkpoint_threads={}'.format(threads)
    new_config = conn_config + extra

    WiredTigerTestCase.verbose(
        None, 3,
        'parallel_checkpoint hook: calling wiredtiger_open({}, {})'
        .format(homedir, new_config))

    return orig_wiredtiger_open(homedir, new_config)

class ParallelCheckpointHookCreator(wthooks.WiredTigerHookCreator):
    def __init__(self, arg=0):
        # Default to 4 parallel checkpoint threads
        self.threads = 4

        params = parse_hook_args(arg)
        if 'threads' in params:
            try:
                self.threads = int(params['threads'])
            except ValueError:
                raise Exception(
                    'hook_parallel_checkpoint: invalid threads value "{}"'
                    .format(params['threads']))
        # Reject unknown parameters
        for key in params:
            if key != 'threads':
                raise Exception(
                    'hook_parallel_checkpoint: unknown parameter "{}"'
                    .format(key))

        self.platform_api = wthooks.DefaultPlatformAPI()

    def get_platform_api(self):
        return self.platform_api

    def register_skipped_tests(self, tests):
        # No tests skipped; add entries here if needed
        pass

    def setup_hooks(self):
        # Replace wiredtiger_open with a wrapper that appends checkpoint_threads
        orig_wiredtiger_open = self.wiredtiger['wiredtiger_open']

        self.wiredtiger['wiredtiger_open'] = (
            wthooks.HOOK_REPLACE,
            lambda homedir, config:
                wiredtiger_open_replace(orig_wiredtiger_open, homedir, config, self.threads)
        )

def initialize(arg):
    return [ParallelCheckpointHookCreator(arg)]
