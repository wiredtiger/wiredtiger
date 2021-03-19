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
#
# WiredTigerHookManager
#   Manage running of hooks
#
from __future__ import print_function

import os, sys
from importlib import import_module
import wiredtiger

# Three kinds of hooks available:
HOOK_REPLACE = 1     # replace the call with the hook function
HOOK_NOTIFY = 2      # call the hook function after the function
HOOK_ARGS = 3        # transform the arg list before the call

def tty(message):
    from wttest import WiredTigerTestCase
    WiredTigerTestCase.tty(message)

# A global function, when called, has no self.  We'll make a "self"
# which will be the wiredtiger module
def hooked_function_for_global_function(orig_func, hooks_name, *args):
    return hooked_function(wiredtiger, orig_func, hooks_name, *args)

def hooked_function(self, orig_func, hooks_name, *args):
    hook_func_list = getattr(self, hooks_name)

    notifies = []
    replace_func = None

    # The three kinds of hooks are acted upon at different times.
    # Before we call the function, we modify the args as indicated
    # by hooks.  Then we call the function, possibly with a replacement.
    # Finally, we'll call any notify hooks.
    #
    # We only walk through the hook list once, and process the config
    # hooks while we're doing that, and copy any other hooks needed.
    for hook_type,hook_func in hook_func_list:
        if hook_type == HOOK_NOTIFY:
            notifies.append(hook_func)
        elif hook_type == HOOK_REPLACE:
            replace_func = hook_func
        elif hook_type == HOOK_ARGS:
            # The arg list may be completely transformed,
            # and multiple hooks may do this.
            args = hook_func(self, args)

    if replace_func == None:
        if self == wiredtiger:
            ret = orig_func(*args)
        else:
            ret = orig_func(self, *args)
    else:
        if self == wiredtiger:
            ret = replace_func(*args)
        else:
            ret = replace_func(self, *args)

    for hook_func in notifies:
        hook_func(ret, self, *args)
    return ret

class WiredTigerHookManager(object):
    def __init__(self, hooknames = []):
        self.hooks = []
        names_seen = []
        for name in hooknames:
            # The hooks are indicated as "somename=arg" or simply "somename".
            # hook_somename.py will be imported, and initialized with the arg.
            # Names must be unique, as we stash some info into extra fields
            # on the connection/session/cursor, these are named using the
            # unique name of the hook.
            if '=' in name:
                name,arg = name.split('=', 1)
            else:
                arg = None
            if name in names_seen:
                raise Exception(name + ': hook name cannot be used multiple times')
            names_seen.append(name)

            modname = 'hook_' + name
            try:
                imported = import_module(modname)
                for hook in imported.initialize(arg):
                    hook._initialize(name, self)
                    self.hooks.append(hook)
            except:
                print('Cannot import hook: ' + name + ', check file ' + modname + '.py')
                raise
        for hook in self.hooks:
            hook.setup_hooks()

    def add_hook(self, clazz, method_name, hook_type, hook_func):
        if not hasattr(clazz, method_name):
            raise Exception('Cannot find method ' + method_name + ' on class ' + str(clazz))

        # Given that the method name is XXXX, and class is Connection, here's what we're doing:
        #    wiredtiger.Connection._XXXX_hooks = []
        #    wiredtiger.Connection._XXXX_orig = wiredtiger.Connection.XXXX
        #    wiredtiger.Connection.XXXX = lambda self, *args: hooked_function(self, 'XXXX', *args)
        hooks_name = '_' + method_name + '_hooks'
        orig_name = '_' + method_name + '_orig'
        if not hasattr(clazz, hooks_name):
            tty('Setting up hook on ' + str(clazz) + '.' + method_name)
            setattr(clazz, hooks_name, [])
            orig_func = getattr(clazz, method_name)
            if orig_func == None:
                raise Exception('method ' + method_name + ' hook setup: method does not exist')

            # If we're using the wiredtiger module and not a class, we need a slightly different style of hooked_function.
            if clazz == wiredtiger:
                f = lambda *args: hooked_function(wiredtiger, orig_func, hooks_name, *args)
            else:
                f = lambda self, *args: hooked_function(self, orig_func, hooks_name, *args)
            setattr(clazz, method_name, f)
            setattr(clazz, orig_name, orig_func)

        # Now add to the list of hook functions
        # If it's a replace hook, we only allow one of them for a given method name
        hooks_list = getattr(clazz, hooks_name)
        if hook_type == HOOK_REPLACE:
            for ht, hf in hooks_list:
                if ht == HOOK_REPLACE:
                    raise Exception('method ' + method_name + ' hook setup: trying to replace the same method with two hooks')
        elif hook_type != HOOK_NOTIFY and hook_type != HOOK_ARGS:
            raise Exception('method ' + method_name + ' hook setup: unknown hook_type: ' + str(hook_type))
        hooks_list.append([hook_type, hook_func])
        setattr(clazz, hooks_name, hooks_list)
        tty('Setting up hooks list in ' + str(clazz) + '.' + hooks_name)

    def get_function(self, clazz, method_name):
        orig_name = '_' + method_name + '_orig'
        if hasattr(clazz, orig_name):
            orig_func = getattr(clazz, orig_name)
        else:
            orig_func = getattr(clazz, method_name)
        return orig_func

    def filter_tests(self, tests):
        for hook in self.hooks:
            tests = hook.filter_tests(tests)
        return tests

# Hooks must derive from this class
class WiredTigerHook(object):
    def __init__(self):
        pass

    # This is called right after creation and should not be overridden.
    def _initialize(self, name, hookmgr):
        self.name = name
        self.hookmgr = hookmgr

    # default version of filter_tests, can be overridden
    def filter_tests(self, tests):
        return tests

    def setup_hooks(self):
        raise Exception('setup_hooks must be overridden by a hook class')

    # Call these to do override of "global" WiredTiger functions, like wiredtiger_open.
    def add_wiredtiger_hook(self, method_name, hook_type, hook_func):
        self.hookmgr.add_hook(wiredtiger, method_name, hook_type, hook_func)

    def add_connection_hook(self, method_name, hook_type, hook_func):
        self.hookmgr.add_hook(wiredtiger.Connection, method_name, hook_type, hook_func)

    def add_session_hook(self, method_name, hook_type, hook_func):
        self.hookmgr.add_hook(wiredtiger.Session, method_name, hook_type, hook_func)

    def add_cursor_hook(self, method_name, hook_type, hook_func):
        self.hookmgr.add_hook(wiredtiger.Cursor, method_name, hook_type, hook_func)

    # Allows lookup of the original version of functions
    def get_wiredtiger_function(self, method_name):
        return self.hookmgr.get_function(wiredtiger, method_name)

    def get_connection_function(self, method_name):
        return self.hookmgr.get_function(wiredtiger.Connection, method_name)

    def get_session_function(self, method_name):
        return self.hookmgr.get_function(wiredtiger.Session, method_name)

    def get_cursor_function(self, method_name):
        return self.hookmgr.get_function(wiredtiger.Cursor, method_name)
