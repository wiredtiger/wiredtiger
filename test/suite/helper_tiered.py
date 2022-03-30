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

import datetime, inspect, os, random 

# These routines help run the various storage sources. They are required to manage
# generation of storage source specific configurations.

# Generate a storage store specific authentication token.
def get_auth_token(storage_source):
    auth_token = None
    if storage_source == 'dir_store':
        # Fake a secret token.
        auth_token = "Secret"
    if storage_source == 's3_store':
        # Auth token is the AWS access key ID and the AWS secret key as semi-colon separated values.
        # We expect the values to have been provided through the environment variables.
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        if access_key and secret_key:
            auth_token = access_key + ";" + secret_key
    return auth_token

# Get buckets configured for the storage source

# S3 buckets with their regions
s3_buckets = ['s3testext;ap-southeast-2', 's3testext-us;us-east-2']

# Local buckets do not have a region
local_buckets = ['bucket1', 'bucket2']

# Get name of the first bucket in the list.
def get_bucket1_name(storage_source):
    if storage_source == 's3_store':
        return s3_buckets[0]
    if storage_source == 'dir_store':
        return local_buckets[0]
    return None

# Get name of the second bucket in the list.
def get_bucket2_name(storage_source):
    if storage_source == 's3_store':
        return s3_buckets[1]
    if storage_source == 'dir_store':
        return local_buckets[1]
    return None

# Generate a unique object prefix for the S3 store. 
def generate_s3_prefix(test_name = ''):
    # Generates a unique prefix to be used with the object keys, eg:
    # "s3test_artefacts/python_2022-31-01-16-34-10_623843294/"
    prefix = 's3test_artefacts--python_'
    prefix += datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    # Range upto int32_max, matches that of C++'s std::default_random_engine
    prefix += '_' + str(random.randrange(1, 2147483646)) + '--'

    # If the calling function has not provided a name, extract it from the stack.
    # It is important to generate unique prefixes for different tests in the same class,
    # so that the database namespace do not collide.
    # 0th element on the stack is the current function. 1st element is the calling function.
    if not test_name:
        test_name = inspect.stack()[1][3]
    prefix += test_name + '--'

    return prefix

# Storage sources
tiered_storage_sources = [
    ('dirstore', dict(auth_token = get_auth_token('dir_store'),
        bucket = get_bucket1_name('dir_store'),
        bucket_prefix = "pfx_",
        ss_name = 'dir_store')),
    ('s3', dict(auth_token = get_auth_token('s3_store'),
        bucket = get_bucket1_name('s3_store'),
        bucket_prefix = generate_s3_prefix(),
        ss_name = 's3_store')),
    ('non_tiered', dict(ss_name = 'non_tiered')),            
]

# Retruns True if the current scenario is tiered
def is_tiered_scenario(test_case):
    res = False
    if hasattr(test_case, 'ss_name'):
        res = test_case.ss_name != 'non_tiered'
    
    return res

# Setup custom connection config.
def tiered_conn_config(test_case):
    # Handle non_tiered storage scenarios.
    if not is_tiered_scenario(test_case):
        return ''

    # Setup directories structure for local tiered storage.
    if test_case.ss_name == 'dir_store' and not os.path.exists(test_case.bucket):
        os.mkdir(test_case.bucket)

    # Build tiered storage connection string.
    return \
        'debug_mode=(flush_checkpoint=true),' + \
        'tiered_storage=(auth_token=%s,' % test_case.auth_token + \
        'bucket=%s,' % test_case.bucket + \
        'bucket_prefix=%s,' % test_case.bucket_prefix + \
        'name=%s),tiered_manager=(wait=0)' % test_case.ss_name

# Load the storage sources extension.
def tiered_conn_extensions(test_case, extlist):
    # Handle non_tiered storage scenarios.
    if not is_tiered_scenario(test_case):
        return ''
    
    config = ''
    # S3 store is built as an optional loadable extension, not all test environments build S3.
    if test_case.ss_name == 's3_store':
        #config = '=(config=\"(verbose=1)\")'
        extlist.skip_if_missing = True
    #if test_case.ss_name == 'dir_store':
        #config = '=(config=\"(verbose=1,delay_ms=200,force_delay=3)\")'
    # Windows doesn't support dynamically loaded extension libraries.
    if os.name == 'nt':
        extlist.skip_if_missing = True
    extlist.extension('storage_sources', test_case.ss_name + config)
