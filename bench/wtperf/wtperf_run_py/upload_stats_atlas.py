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

import os
import sys
import argparse
from pymongo import MongoClient
import json
import datetime
from pymongo.collection import Collection


def upload_results(coll: Collection, file: str, branch: str):
    with open(file, 'r') as file:
        file_data = json.load(file)
        file_data['Branch'] = branch
        file_data['Timestamp'] = datetime.datetime.now()
        coll.insert_one(file_data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--username', help='Atlas User name')
    parser.add_argument('-p', '--password', help='Atlas password')
    parser.add_argument('-f', '--file',
                        help='path of the JSON file to upload in the cluster'
                        )
    parser.add_argument('-c', '--collection', help='collection name')
    parser.add_argument('-b', '--branch', help='branch name')

    args = parser.parse_args()

    if args.username is None or args.password is None:
        sys.exit('Atlas Username and Password is required')
    if args.file is None:
        sys.exit('The path to the file is required')
    if os.path.isfile(args.file) is False:
        sys.exit('{} file does not exist'.format(args.file))
    if args.collection is None:
        sys.exit('The name of the Atlas collection is required')

    CLUSTER = 'wtperf.5bfc8.mongodb.net'
    DATABASE = 'PerformanceTests'

    client_str = 'mongodb+srv://' + args.username + ':' + args.password \
        + '@' + CLUSTER + '/' + DATABASE \
        + '?retryWrites=true&w=majority'

    client = MongoClient(client_str)

    databases = client.database_names()
    if DATABASE not in databases:
        sys.exit('Database: {}, does not exist in Atlas Cluster'.format(DATABASE))
    db = client[DATABASE]

    collections = db.collection_names()
    if args.collection not in collections:
        sys.exit('Collection/Test: {}, does not exist in Atlas Cluster'.format(args.collection))
    coll = db[args.collection]

    upload_results(coll, args.file, args.branch)


if __name__ == '__main__':
    main()
