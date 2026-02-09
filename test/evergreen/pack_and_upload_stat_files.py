#!/usr/bin/env python3
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
import os
import re
import tarfile
import boto3

def targz_pack_stat_files(source_dir, regex):
    target = source_dir.replace("/", ".") + ".tar.gz"
    with tarfile.open(target, "w:gz") as tar:  
        for root, dirs, files in os.walk(source_dir):  
            for file in files:
                if regex.match(file):
                    file_path = os.path.join(root, file)  
                    tar.add(file_path, arcname=os.path.relpath(file_path, source_dir))
    return target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--aws-secret', required=True, help='AWS secret access key')
    parser.add_argument('-k', '--aws-key', required=True, help='AWS access key ID')
    parser.add_argument('-r', '--remote-file-prefix', required=True, help='Path to the file\'s location in S3')
    args = parser.parse_args()

    aws_secret = args.aws_secret
    aws_key = args.aws_key
    remote_file_prefix = args.remote_file_prefix

    s3 = boto3.client(  
        's3',  
        aws_access_key_id=aws_key,  
        aws_secret_access_key=aws_secret  
    )  

    regex = re.compile(r'WiredTigerStat.*')
    for root, _, files in os.walk("."):
        for file in files:
            if regex.match(file):

                # If current directory contains any stat files, pack them all into a tar.gz (one archive per directory).
                target = targz_pack_stat_files(root, regex)

                # Upload the tar.gz to S3.
                with open(target, "rb") as f:  
                    s3.put_object(  
                        Bucket="build_external",  
                        Key=remote_file_prefix + "_" + target,  
                        Body=f,  
                        ACL="public-read",  
                        ContentType="application/tar",  
                        Metadata={"DisplayName": "Artifacts"} 
                    )
                    
                break

if __name__ == "__main__":
    main()
