#!/usr/bin/env python3

import argparse
import re

from enum import Enum

class UpdateType(Enum):
    INIT = 1
    TREE = 2
    UNKNOWN = 3

class Update:
    def __init__(self, update_type, line):
        self.type = update_type
        self.line = line

        if self.type == UpdateType.INIT:
            self.init_init(line)

    def init_init(self, line):
        matches = re.search('stable_timestamp=\((\d+), (\d+)\)', line)
        self.stable_txn_id = matches.group(1)
        self.stable_ts = matches.group(2)

class Checker:
    def __init__(self):
        self.stable_txn_id = None
        self.stable_ts = None

    def apply(self, update):
        if update.type == UpdateType.INIT:
            self.apply_check_init(update)
        if update.type == UpdateType.UNKNOWN:
            raise Exception(f"failed to parse {update.line}")
        else:
            pass

    def apply_check_init(self, update):
        if self.stable_txn_id is not None:
            raise Exception("restarted RTS?!")
        if self.stable_ts is not None:
            raise Exception("restarted RTS?!")

        self.stable_txn_id = update.stable_txn_id
        self.stable_ts = update.stable_ts

def init_to_update(line):
    return Update(UpdateType.INIT, line)

def tree_to_update(line):
    return Update(UpdateType.TREE, line)

def parse_to_update(line):
    if 'INIT' in line:
        return init_to_update(line)
    elif 'TREE' in line:
        return tree_to_update(line)
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
