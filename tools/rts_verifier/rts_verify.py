#!/usr/bin/env python3

import argparse

from checker import Checker
from update import Update, UpdateType

def parse_to_update(line):
    if '[INIT]' in line:
        return Update(UpdateType.INIT, line)
    elif '[TREE]' in line:
        return Update(UpdateType.TREE, line)
    elif '[TREE_LOGGING]' in line:
        return Update(UpdateType.TREE_LOGGING, line)
    elif '[PAGE_ROLLBACK]' in line:
        return Update(UpdateType.PAGE_ROLLBACK, line)
    elif '[UPDATE_ABORT]' in line:
        return Update(UpdateType.UPDATE_ABORT, line)
    else:
        return Update(UpdateType.UNKNOWN, line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verify actions taken by rollback to stable from verbose messages.')
    parser.add_argument('file', type=str, help='the log file to parse verbose messages from')
    args = parser.parse_args()

    checker = Checker()
    with open(args.file) as f:
        for line in f:
            if 'WT_VERB_RTS' in line:
                update = parse_to_update(line)
                checker.apply(update)
