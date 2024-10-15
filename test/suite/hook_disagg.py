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

# hook_disagg.py
#
# Substitute oligarch tables for regular (row-store) tables in Python tests.
#
# These hooks are intended for basic testing.  There are several
# pieces of functionality here.
#
# 1. We add disaggregated storage parameters to the config when calling wiredtiger_open()
#
# 2. When creating a table, we add options to use the page_log service.
#
# 3. We stub out some functions that aren't supported by disaggregated storage.
#
# To run, for example, the cursor tests with these hooks enabled:
#     ../test/suite/run.py --hook disagg cursor
#
from __future__ import print_function

import os, re, unittest, wthooks, wttest
from wttest import WiredTigerTestCase
from helper_disagg import DisaggConfigMixin, gen_disagg_storages

# These are the hook functions that are run when particular APIs are called.

# Add the local storage extension whenever we call wiredtiger_open
def wiredtiger_open_disagg(ignored_self, args):

    disagg_storage_sources = gen_disagg_storages()
    testcase = WiredTigerTestCase.currentTestCase()
    extension_name = testcase.getDisaggService()
    extension_config = testcase.getDisaggConfig()

    valid_storage_source = False
    # Construct the configuration string based on the storage source.
    for disagg_details in disagg_storage_sources:
        if (testcase.getDisaggService() == disagg_details[0]):
            valid_storage_source = True

    if valid_storage_source == False:
        raise AssertionError('Invalid storage source passed in the argument.')

    curconfig = args[-1]
    homedir = args[0]

    # If there is already disagg storage enabled, we shouldn't enable it here.
    # We might attempt to let the wiredtiger_open complete without alteration,
    # however, we alter several other API methods that would do weird things with
    # a different disagg_storage configuration. So better to skip the test entirely.
    if 'oligarch=' in curconfig:
        skip_test("cannot run disagg hook on a test that already uses disagg storage")

    # Similarly if this test is already set up to run disagg vs non-disagg scenario, let's
    # not get in the way.
    if hasattr(testcase, 'disagg_conn_config'):
        skip_test("cannot run disagg hook on a test that already includes DisaggConfigMixin")

    if 'in_memory=true' in curconfig:
        skip_test("cannot run disagg hook on a test that is in-memory")

    # Mark this test as readonly, but don't disallow it.  See testcase_is_readonly().
    if 'readonly=true' in curconfig:
        testcase._readonlyDisaggTest = True

    extension_libs = WiredTigerTestCase.findExtension('page_log', extension_name)
    if len(extension_libs) == 0:
        raise Exception(extension_name + ' storage source extension not found')

    disagg_config = ',log=(enabled),transaction_sync=(enabled,method=fsync),statistics=(all),statistics_log=(wait=1,json=true,on_close=true)'
    disagg_config += ',oligarch=(role="leader")'

    # Build the extension strings, we'll need to merge it with any extensions
    # already in the configuration.
    ext_string = 'extensions=['
    start = curconfig.find(ext_string)
    if start >= 0:
        end = curconfig.find(']', start)
        if end < 0:
            raise Exception('hook_disagg: bad extensions in config \"%s\"' % curconfig)
        ext_string = curconfig[start: end]

    if extension_config == None:
        ext_lib = '\"%s\"' % extension_libs[0]
    else:
        ext_lib = '\"%s\"=(config=\"%s\")' % (extension_libs[0], extension_config)

    disagg_config += ',' + ext_string + ',%s]' % ext_lib

    args = list(args)           # convert from a readonly tuple to a writeable list
    args[-1] += disagg_config   # Modify the list

    WiredTigerTestCase.verbose(None, 3,
        '    Calling wiredtiger_open with config = \'{}\''.format(args))

    return args

# We want readonly tests to run with disagg storage, since it is possible to do readonly
# operations.  This function is called for two purposes:
#  - when readonly is enabled, we don't want to do flush_tier calls.
#  - normally the hook silently removes other (not supported) calls, like compact/salvage.
#    Except that some tests enable readonly and call these functions, expecting an exception.
#    So for these "modifying" APIs, we want to actually do the operation (but only when readonly).
def testcase_is_readonly():
    testcase = WiredTigerTestCase.currentTestCase()
    return getattr(testcase, '_readonlyDisaggTest', False)

def testcase_has_failed():
    testcase = WiredTigerTestCase.currentTestCase()
    return testcase.failed()

def testcase_has_skipped():
    testcase = WiredTigerTestCase.currentTestCase()
    return testcase.skipped

def skip_test(comment):
    testcase = WiredTigerTestCase.currentTestCase()
    testcase.skipTest(comment)

# Called to replace Connection.close
# Insert a call to flush_tier before closing connection.
def connection_close_replace(orig_connection_close, connection_self, config):
    # We cannot call flush_tier on a readonly connection.
    # Likewise we should not call flush_tier if the test case has failed,
    # and the connection is being closed at the end of the run after the failure.
    # Otherwise, diagnosing the original failure may be troublesome.
    if not testcase_is_readonly() and not testcase_has_failed() and \
      not testcase_has_skipped():
        s = connection_self.open_session(None)
        s.checkpoint('flush_tier=(enabled,force=true)')
        s.close()

    ret = orig_connection_close(connection_self, config)
    return ret

# Called to replace Session.checkpoint.
# We add a call to flush_tier during every checkpoint to make sure we are exercising disagg
# functionality.
def session_checkpoint_replace(orig_session_checkpoint, session_self, config):
    # FIXME-WT-10771 We cannot do named checkpoints with disagg storage objects.
    # We can't really continue the test without the name, as the name will certainly be used.
    if config == None:
        config = ''
    if 'name=' in config:
        skip_test('named checkpoints do not work in disagg storage')
    # We cannot call flush_tier on a readonly connection.
    if not testcase_is_readonly():
        # Enable flush_tier on checkpoint.
        config += ',flush_tier=(enabled,force=true)'
    return orig_session_checkpoint(session_self, config)

# Called to replace Session.compact
def session_compact_replace(orig_session_compact, session_self, uri, config):
    # FIXME-PM-2538
    # Compact isn't implemented for disagg tables.  Only call it if this can't be the uri
    # of a disagg table.  Note this isn't a precise match for when we did/didn't create
    # a disagg table, but we don't have the create config around to check.
    # Background compaction can be enabled or disabled, each compact call it issues should circle
    # back here.
    # We want readonly connections to do the real call, see comment in testcase_is_readonly.
    ret = 0
    background_compaction = not uri and config and "background=" in config
    if background_compaction or not uri.startswith("table:") or testcase_is_readonly():
        ret = orig_session_compact(session_self, uri, config)
    return ret

# Called to replace Session.create
def session_create_replace(orig_session_create, session_self, uri, config):
    if config == None:
        new_config = ""
    else:
        new_config = config

    # If the test isn't creating a table (i.e., it's a column store or lsm) create it as a
    # "local only" object.  Otherwise we get disagg storage from the connection defaults.
    # We want readonly connections to do the real call, see comment in testcase_is_readonly.
    # FIXME-WT-9832 Column store testing should be allowed with this hook.
    if uri.startswith("table:") and not "key_format=r" in new_config and not "type=lsm" in new_config and not testcase_is_readonly():
        new_config += ',stable_prefix=.,page_log=palm'

    WiredTigerTestCase.verbose(None, 3,
        '    Creating \'{}\' with config = \'{}\''.format(uri, new_config))
    ret = orig_session_create(session_self, uri, new_config)
    return ret

# FIXME-PM-2532
# Called to replace Session.open_cursor. This is needed to skip tests that
# do backup on (disagg) table data sources, as that is not yet supported.
def session_open_cursor_replace(orig_session_open_cursor, session_self, uri, dupcursor, config):
    if uri != None and uri.startswith("backup:"):
        skip_test("backup on disagg tables not yet implemented")
    return orig_session_open_cursor(session_self, uri, dupcursor, config)

# Called to replace Session.salvage
def session_salvage_replace(orig_session_salvage, session_self, uri, config):
    # Salvage isn't implemented for disagg tables.  Only call it if this can't be the uri
    # of a disagg table.  Note this isn't a precise match for when we did/didn't create
    # a disagg table, but we don't have the create config around to check.
    # We want readonly connections to do the real call, see comment in testcase_is_readonly.
    ret = 0
    if not uri.startswith("table:") or testcase_is_readonly():
        ret = orig_session_salvage(session_self, uri, config)
    return ret

# Called to replace Session.verify
def session_verify_replace(orig_session_verify, session_self, uri, config):
    # Verify isn't implemented for disagg tables.  Only call it if this can't be the uri
    # of a disagg table.  Note this isn't a precise match for when we did/didn't create
    # a disagg table, but we don't have the create config around to check.
    # We want readonly connections to do the real call, see comment in testcase_is_readonly.
    ret = 0
    if not uri.startswith("table:") or testcase_is_readonly():
        ret = orig_session_verify(session_self, uri, config)
    return ret

# Every hook file must have one or more classes descended from WiredTigerHook
# This is where the hook functions are 'hooked' to API methods.
class DisaggHookCreator(wthooks.WiredTigerHookCreator):
    def __init__(self, arg=0):
        # Caller can specify an optional command-line argument.  We're not using it
        # now, but this is where it would show up.

        # Override some platform APIs
        self.platform_api = DisaggPlatformAPI(arg)

    # Our current disagg storage implementation has a slow version of truncate, and
    # some tests are sensitive to that.
    #
    # FIXME-WT-11023: when we implement a fast truncate for disagg storage, we might remove
    # this, and visit tests that are marked with @wttest.prevent(..."slow_truncate"...)
    def uses(self, use_list):
        if "slow_truncate" in use_list:
            return True
        return False

    # Determine whether a test should be skipped, if it should also return the reason for skipping.
    # Some features aren't supported with disagg storage currently. If they exist in
    # the test name (or scenario name) skip the test.
    def should_skip(self, test) -> (bool, str):
        skip_categories = [
            ("backup",               "Can't backup a disagg table"),
            ("inmem",                "In memory tests don't make sense with disagg storage"),
            ("lsm",                  "LSM is not supported with tiering"),
            ("modify_smoke_recover", "Copying WT dir doesn't copy the bucket directory"),
            ("test_salvage",         "Salvage tests directly name files ending in '.wt'"),
            ("test_config_json",     "Disagg hook's create function can't handle a json config string"),
            ("test_cursor_big",      "Cursor caching verified with stats"),
            ("disagg",               "Disagg tests already do tiering."),
            ("test_verify",          "Verify not supported on disagg tables (yet)")
        ]

        for (skip_string, skip_reason) in skip_categories:
            if skip_string in str(test):
                return (True, skip_reason)

        return (False, None)

    # Skip tests that won't work on disagg cursors
    def register_skipped_tests(self, tests):
        for t in tests:
            (should_skip, skip_reason) = self.should_skip(t)
            if should_skip:
                wttest.register_skipped_test(t, "disagg", skip_reason)

    def get_platform_api(self):
        return self.platform_api

    def setup_hooks(self):
        orig_connection_close = self.Connection['close']
        self.Connection['close'] = (wthooks.HOOK_REPLACE, lambda s, config=None:
          connection_close_replace(orig_connection_close, s, config))

        orig_session_checkpoint = self.Session['checkpoint']
        self.Session['checkpoint'] =  (wthooks.HOOK_REPLACE, lambda s, config=None:
            session_checkpoint_replace(orig_session_checkpoint, s, config))

        orig_session_compact = self.Session['compact']
        self.Session['compact'] =  (wthooks.HOOK_REPLACE, lambda s, uri, config=None:
          session_compact_replace(orig_session_compact, s, uri, config))

        orig_session_create = self.Session['create']
        self.Session['create'] =  (wthooks.HOOK_REPLACE, lambda s, uri, config=None:
          session_create_replace(orig_session_create, s, uri, config))

        orig_session_open_cursor = self.Session['open_cursor']
        self.Session['open_cursor'] = (wthooks.HOOK_REPLACE, lambda s, uri, todup=None, config=None:
          session_open_cursor_replace(orig_session_open_cursor, s, uri, todup, config))

        orig_session_salvage = self.Session['salvage']
        self.Session['salvage'] = (wthooks.HOOK_REPLACE, lambda s, uri, config=None:
          session_salvage_replace(orig_session_salvage, s, uri, config))

        orig_session_verify = self.Session['verify']
        self.Session['verify'] = (wthooks.HOOK_REPLACE, lambda s, uri, config=None:
          session_verify_replace(orig_session_verify, s, uri, config))

        self.wiredtiger['wiredtiger_open'] = (wthooks.HOOK_ARGS, wiredtiger_open_disagg)

# Strip matching parens, which act as a quoting mechanism.
def strip_matching_parens(s):
    if len(s) >= 2:
        if s[0] == '(' and s[-1] == ')':
            s = s[1:-1]
    return s

def config_split(config):
    pos = config.index('=')
    if pos >= 0:
        left = config[:pos]
        right = config[pos+1:]
    else:
        left = config
        right = ''
    return [left, strip_matching_parens(right)]

# Override some platform APIs for this hook.
class DisaggPlatformAPI(wthooks.WiredTigerHookPlatformAPI):
    def __init__(self, arg=None):
        params = []

        # We want to split args something like arg.split(','), except that we need
        # to sometimes allow commas as part of the individual parameters, which we
        # allow via parens.  For example, a developer can run:
        #  run.py --hook \
        #    'disagg=(tier_storage_source=dir_store,tier_storage_source_config=(force_delay=5,delay_ms=10))'
        #
        # and that should appear as two parameters to the disagg hook.
        if arg:
            arg = strip_matching_parens(arg)
            # Note: this regular expression does not handle arbitrary nesting of parens
            config_list = re.split(r",(?=(?:[^(]*[(][^)]*[)])*[^)]*$)", arg)
            params = [config_split(config) for config in config_list]

        import wttest
        #wttest.WiredTigerTestCase.tty('Disagg hook params={}'.format(params))
        for param_key, param_value in params:
            # no parameters yet
            raise Exception('hook_disagg: unknown parameter {}'.format(param_key))
        self.disagg_service = 'palm'
        self.disagg_config = ''

    def tableExists(self, name):
        # TODO: for palm will need to rummage in PALM files.
        return False

    def initialFileName(self, uri):
        # TODO: there really isn't an equivalent
        # return 'kv_home/data.mdb'
        return None

    def getDisaggService(self):
        return self.disagg_service
        
    def getDisaggConfig(self):
        return self.disagg_config

# Every hook file must have a top level initialize function,
# returning a list of WiredTigerHook objects.
def initialize(arg):
    return [DisaggHookCreator(arg)]
