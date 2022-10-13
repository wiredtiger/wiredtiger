#!/usr/bin/python3
import bson
import codecs
import pprint
import re
import sys
#import logging

# This script is intended to parse the output of three wt util commands and convert MongoDB bson
# from hexadecimal or byte format into ascii.
#
# It currently works with the following wt util commands:
# - dump
# - verify
# - printlog
#
# Those tools each perform a different function, and their usages are varied as such the script
# needs to handle their output separately. Originally many scripts existed for this purpose, the
# intent of this script is to provide a single place to perform all bson conversions

# This function provides a consistent bson pretty print format.
def print_bson(bson):
    return pprint.pformat(bson, indent=1).replace('\n', '\n\t  ')

# A utility function for converting verify byte output into parsible hex.
def get(inp):
    ret = ""
    idx = 0
    while True:
        if idx >= len(inp):
            break
        ch = inp[idx]
        if ord(ch) != 92:
            ret += ch
            idx += 1
            continue
        lookAhead = inp[idx+1]
        if ord(lookAhead) != 92:
            ret += ch + 'x'
            idx += 1
            continue
        ret += ch + ch
        idx += 2
    return codecs.escape_decode(ret)[0]

# Converts the output of ./wt printlog -x.
# Doesn't convert hex keys as keys currently I don't think they are bsons?
def wt_printlog_to_bson():
    pattern_value = re.compile('value-hex\": \"(.*)\"')
    pattern_key = re.compile('key-hex\": \"(.*)\"')
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        value_match = pattern_value.search(line)
        if value_match:
            value_hex_str = value_match.group(1)
            value_bytes = bytes.fromhex(value_hex_str)
            try:
                bson_obj = bson.decode_all(value_bytes)
                print('\t\"value-bson\":%s' % (print_bson(bson_obj),))
            except Exception as e:
                # If bsons don't appear to be printing uncomment this line for the error reason.
                #logging.error('Error at %s', 'division', exc_info=e)
                print('\t\"value-hex\": \"' + value_hex_str + '\"')
        else:
            print(line, end='')

# Converts the output of ./wt dump -x to bson.
def wt_dump_to_bson():
    # Re print all the metadata information printed by ./wt dump.
    while True:
        line = sys.stdin.readline()
        line = line.strip()
        if not line:
            print("Unexpectedly empty data section. Are you sure the wt data file has content?")
            sys.exit(1)
        print(line)
        if line == 'Data':
            break
    # Parse key/value pairs and convert the values to bson.
    while True:
        key = sys.stdin.readline()
        if not key:
            break
        key = key.strip()
        value = sys.stdin.readline().strip()
        print('Key:\t%s' % (key,))
        byt = bytes.fromhex(value)
        obj = bson.decode_all(byt)
        print('Value:\n\t%s' % (print_bson(obj),))

# Converts the output of ./verify -d dump_pages to bson.
def wt_verify_to_bson():
    pattern = re.compile('V {(.*?)}$')
    while True:
        line = sys.stdin.readline()
        if not line:
            break

        matches = pattern.findall(line.strip())
        if not matches:
            print(line, end='')
            continue

        obj = bson.decode_all(get(matches[0]))[0]
        print(line, end='')
        print('\t  %s' % (print_bson(obj),))

# Usage message.
def usage():
    print('usage: ./wt [verify|dump|printlog] | ./hex_to_bson.py {-d -v -p}')
    print('\t This script is expecting to read in the output from the wt util.')
    print('\t-d parses output of wt dump -x')
    print('\t-v parses output of wt verify -d dump_pages')
    print('\t-p parses output of wt printlog -x')

def main():
    if len(sys.argv) == 1:
        usage()
        exit(1)
    type = sys.argv[1]
    if type == '-d':
        wt_dump_to_bson()
    elif type == '-v':
        wt_verify_to_bson()
    elif type == '-p':
        wt_printlog_to_bson()
    else:
        usage()
        exit(1)

if __name__ == "__main__":
    main()
