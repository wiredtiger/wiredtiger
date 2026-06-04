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

"""Named test suites for run.py. Usage:

    python3 ../test/suite/run.py disagg     # multi-pass: see 'disagg' below
    python3 ../test/suite/run.py tiered
    python3 ../test/suite/run.py classic

To add a suite, add an entry to NAMED_SUITES with:
  'prefixes':       tuple of test_*.py prefixes the suite covers, OR
  'exclude_suites': tuple of other suite names this suite is the complement of
  'hooks':          extra hook configurations to test. For each entry,
                    run.py is re-launched with --hook applied. An entry is:
                      a hook spec string                run over every test
                      {'hook': spec, 'tests': (...)}    run only over the
                                                        listed tests
"""

import glob
import os
import subprocess
import sys

SUITE_DIR = os.path.dirname(os.path.abspath(__file__))

NAMED_SUITES = {
    'disagg': {
        'prefixes': ('test_layered', 'test_disagg'),
        'hooks': (
            'disagg=(role=leader)',
            'disagg=(role=leader,table_prefix=table)',
            # Follower-role passes are pinned to base01 only.
            {'hook': 'disagg=(role=follower)',                    'tests': ('base01',)},
            {'hook': 'disagg=(role=follower,table_prefix=table)', 'tests': ('base01',)},
        ),
    },
    'tiered': {
        'prefixes': ('test_tiered',),
        'hooks':    ('tiered',),
    },
    'classic': {
        'exclude_suites': ('disagg', 'tiered'),
        'hooks':          (),
    },
}

def _all_test_files():
    """All test_*.py filenames (basenames) under test/suite."""
    return [os.path.basename(p)
            for p in glob.glob(os.path.join(SUITE_DIR, 'test_*.py'))]

def test_files_for_suite(name):
    """Return the list of test_*.py filenames that belong to the named
    suite, or None if `name` isn't a registered suite."""
    entry = NAMED_SUITES.get(name)
    if entry is None:
        return None
    all_files = _all_test_files()

    # 'prefixes' suite: keep files whose name starts with any of the
    # listed prefixes.
    if 'prefixes' in entry:
        suite_prefixes = entry['prefixes']
        return [f for f in all_files if f.startswith(suite_prefixes)]

    # 'exclude_suites' suite: this suite is the complement of one or more
    # other suites. Collect every prefix from those other suites, then
    # keep test files that DON'T start with any of them.
    excluded_prefixes = []
    for other_suite_name in entry['exclude_suites']:
        excluded_prefixes.extend(NAMED_SUITES[other_suite_name]['prefixes'])
    excluded_prefixes = tuple(excluded_prefixes)
    return [f for f in all_files if not f.startswith(excluded_prefixes)]

def fail_list_paths_for_hooks(hook_names):
    """For each active hook, look for a sibling skip list at
    fail_lists/hook_<bare_hook_name>.fail. Return the paths that exist."""
    existing_paths = []
    for hook_spec in hook_names:
        bare_name = hook_spec.split('=', 1)[0]
        candidate = os.path.join(SUITE_DIR, 'fail_lists',
                                 f'hook_{bare_name}.fail')
        if os.path.isfile(candidate):
            existing_paths.append(candidate)
    return existing_paths

def hook_passes_for_suite(name):
    """Return the list of subprocess passes that the suite's 'hooks' tuple
    implies. Each pass is a (hook_spec, tests) tuple where:
        hook_spec  is the --hook argument string for that pass
        tests      is None (= run all tests via discovery), or a tuple of
                   testargs to pin the pass to specific tests."""
    entry = NAMED_SUITES.get(name) or {}
    passes = []
    for hook in entry.get('hooks', ()):
        if isinstance(hook, str):
            # Plain string: run all tests under this hook.
            passes.append((hook, None))
        else:
            # Dict form: pin this pass to the listed tests.
            passes.append((hook['hook'], tuple(hook['tests'])))
    return passes

def run_multi_pass_suite(suite_name, forwarded_argv, script_path):
    """Run the suite as a sequence of run.py subprocess invocations:
        pass 1   = the suite's native files, no hook
        pass 2.. = one per entry in the suite's 'hooks' tuple
    forwarded_argv is the user's other run.py flags (-j, -v, ...) which
    are passed unchanged to every subprocess. Fail-fast: stops at the
    first non-zero exit and returns it; returns 0 if every pass succeeds."""

    # Build the list of passes. Each is (label, hook-or-None, tests-or-None):
    #   label = short string for the progress banner
    #   hook  = --hook spec to add (None for the native pass)
    #   tests = list of test args to add (None means let run.py discover)
    passes = []
    native_files = test_files_for_suite(suite_name) or []
    if native_files:
        passes.append(('native', None, native_files))
    for hook_spec, pinned_tests in hook_passes_for_suite(suite_name):
        tests = list(pinned_tests) if pinned_tests else None
        passes.append((f'hook {hook_spec}', hook_spec, tests))

    # Run each pass as a fresh run.py invocation.
    base_cmd = [sys.executable, script_path, *forwarded_argv]
    for i, (label, hook, tests) in enumerate(passes, start=1):
        cmd = list(base_cmd)
        if hook is not None:
            cmd += ['--hook', hook]
        if tests is not None:
            cmd += tests
        test_count = len(tests) if tests else 'discovery'
        header = f'>>> [{suite_name} pass {i}/{len(passes)}] {label}'
        print(f'{header} ({test_count} tests)', flush=True)
        rc = subprocess.call(cmd)
        if rc != 0:
            print(f'{header} FAILED exit={rc}', flush=True)
            return rc
    return 0

def is_multi_pass_invocation(testargs, hook_names):
    """True iff the run.py invocation should hand off to multi-pass
    execution rather than running normally. Triggers when the user named a
    suite that has 'hooks' and did not supply their own --hook."""
    if hook_names:
        # User picked a specific hook config; honor it, don't multi-pass.
        return False
    return any(a in NAMED_SUITES and NAMED_SUITES[a].get('hooks')
               for a in testargs)

def dispatch_multi_pass(testargs, argv):
    """Run the multi-pass suite found in testargs. Returns the exit code
    of the first failing pass, or 0 if all passes succeed. Caller should
    sys.exit with the returned value.

    Errors out via sys.exit if testargs contains more than one named suite
    that implies hooks."""
    multi_pass_suites = [a for a in testargs
                         if a in NAMED_SUITES
                         and NAMED_SUITES[a].get('hooks')]
    if len(multi_pass_suites) > 1:
        sys.exit('cannot combine multiple named suites that imply hooks: '
                 + ', '.join(multi_pass_suites))
    suite = multi_pass_suites[0]
    # Re-build the child argv by stripping the suite name; every other
    # flag the user passed is forwarded as-is.
    forwarded = [a for a in argv[1:] if a != suite]
    return run_multi_pass_suite(suite, forwarded, argv[0])
