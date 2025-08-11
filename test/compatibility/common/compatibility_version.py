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

import re
from typing import Union

# This is a version class aimed to provide a operatable version object
# For compare, this > develop > mongodb-{major}:{minor}
# For invalid version string, this will return false by bool operator

class WTVersion:

    def __init__(self, name:str):
        self.name = name
        self.is_valid = True
        # group is used to unify this and develop
        # this : 2, develop : 1, mongod-a.b : 0
        self.group = 0
        self.main = 0
        self.sub = 0
        if name == "this":
            self.group = 2
        if name == "develop":
            self.group = 1
        else:
            # Match patterns like mongodb-8.0, without prefix and suffix
            match = re.match(r'^mongodb-(\d+)\.(\d+)$', name)
            if match:
                self.main = int(match.group(1))
                self.sub = int(match.group(2))
            else:
                self.is_valid = False
    
    def __bool__(self):
        return self.is_valid
    
    # string format others will directly compare the name
    def __eq__(self, other : Union["WTVersion", str]):
        if isinstance(other, str):
            return self.name == other
        else:
            return (
                self.group == other.group 
                and self.main == other.main 
                and self.sub == other.sub
            )
    
    def __lt__(self, other: "WTVersion"):
        if self.group < other.group:
            return True
        elif self.group > other.group:
            return False
        elif self.main < other.main:
            return True
        elif self.main > other.main:
            return False
        else:
            return self.sub < other.sub
    
    def __gt__(self, other: "WTVersion"):
        if self < other or self == other:
            return False
        else:
            return True

    # Hash is used to support set collection, make it able to remove redundant
    def __hash__(self):
        return hash(self.name)    
    
    def __str__(self):
        return self.name