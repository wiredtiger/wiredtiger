#!/usr/bin/env python3

# This a simple limited replacement for GNU grep
#  - All options must go in the first arg
#  - Order of options matters! - see source

import re, sys

def matchLine(regex, line):
    match = regex.search(line)
    if bool(match) != bool(inverse): print(line, end="")

def matchSearch(regex, line):
    for match in regex.finditer(line):
        print(match if isinstance(match, str) else match[0])

inverse, match, opts, args = False, matchLine, "", sys.argv[1:]
if args[0][0] == "-": opts, args = args[0], args[1:]
regex = "|".join(args)

for opt in [c for c in opts]:
    if opt == 'e': regex = "|".join(args)
    elif opt == 'f': regex = "|".join([re.escape(arg) for arg in args])
    elif opt == 'w': regex = '\\b(?:' + regex + ')\\b'
    elif opt == 'v': inverse = True; match = matchLine;
    elif opt == 'o': match = matchSearch

regex = re.compile(regex)

for line in sys.stdin:
    match(regex, line)

