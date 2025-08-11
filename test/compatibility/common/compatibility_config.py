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

# Common configuration for compatibility tests.

import os
import re
from typing import Dict, List

from compatibility_version import WTVersion

# The branches we use for the testing. We support special branch name 'this' that refers to the
# current branch. This is useful when debugging a compatibility issue on the current branch, but it
# should not be enabled when testing on Evergreen.

# To make this branches compatitable with existing "compatibility_test_for_releases.sh", the
# version is imported from the bash file of "meta/versions.sh"

def extract_versions(bash_script):
    version_ret = {}
    with open(bash_script, "r") as f:
        lines = f.read().split('\n')
    for line in lines:
        match = re.match(r'export\s+([A-Z_]+)\s*=\s*"([^"]*)"', line)
        if match:
            name = match.group(1)
            versions = match.group(2).split()
            versions = [WTVersion(version) for version in versions]
            version_ret[name] = [version for version in versions if version]
    if not version_ret:
        raise Exception("Failed to extract versions from " + bash_script)
    return version_ret

META_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'meta')
BRANCHES : Dict[str, List[WTVersion]] = extract_versions(os.path.join(META_DIR, "versions.sh"))

# Example use of the 'this' branch (useful for debugging):
# BRANCHES = {'SUITE_RELEASE_BRANCHES' : ['this', 'mongodb-7.0']}

# The default directory to which the test will check out other branches, relative to the project's
# top-level directory.
#
# We should reuse the same directory across multiple tests, so that we can check out and build each
# branch only once. And we'd like this directory to be relative to the project's top-level directory
# (as opposed to the current directory), because it simplifies the code, as we change the working
# directory many times during the test run.
BRANCHES_DIR = 'COMPATIBILITY_TEST'
