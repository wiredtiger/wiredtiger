#!/usr/bin/python
# -*- coding: utf-8 -*-

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

import argparse
import json


def main():
    """
    Validate the expected output from the last run of wtperf_run.py
    Take in a list of stats and their values as json and verify them against the contents of evergreen_out.json.
    Example usage:
        ./validate_perf_stats.py '{"Pages seen by eviction": 200, "Insert count": 1153326}'
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('expected_stats', type=str, help='The expected stats and their values')
    args = parser.parse_args()

    expected_stats = json.loads(args.expected_stats)

    stat_file = json.load(open("./test_stats/evergreen_out.json", 'r'))
    test_output = {stat['name']: stat['value'] for stat in stat_file[0]["metrics"]}

    errors = []
    for stat in expected_stats:
        if stat not in test_output:
            errors.append(f"'{stat}'\t not present in evergreen_out.json")
        elif expected_stats[stat] != test_output[stat]:
            errors.append(f"'{stat}'\t value mismatch. Expected: {expected_stats[stat]}, Actual: {test_output[stat]}")

    if errors:
        print("ERROR: Expected values not found:")
        print('\n'.join(errors))
        exit(1)
    else:
        print("Expected stats and values found")


if __name__ == '__main__':
    main()
