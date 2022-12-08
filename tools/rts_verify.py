#!/usr/bin/env python3

import argparse
import re

from enum import Enum

class UpdateType(Enum):
    INIT = 0
    TREE = 1
    TREE_LOGGING = 2
    PAGE_ROLLBACK = 3
    UPDATE_ABORT = 4
    UNKNOWN = 5

class PrepareState(Enum):
    PREPARE_INIT = 0
    PREPARE_INPROGRESS = 1
    PREPARE_LOCKED = 2
    PREPARE_RESOLVED = 3

class Timestamp():
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def __eq__(self, other):
        return self.start == other.start and self.stop == other.stop

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, rhs):
        return ((self.start, self.stop) < (rhs.start, rhs.stop))

    def __le__(self, rhs):
        return self.__lt__(rhs) or self.__eq__(rhs)

    def __gt__(self, rhs):
        return ((self.start, self.stop) > (rhs.start, rhs.stop))

    def __ge__(self, rhs):
        return self.__gt__(rhs) or self.__eq__(rhs)

    def __repr__(self):
        return f"({self.start}, {self.stop})"

class Update:
    def __init__(self, update_type, line):
        self.type = update_type
        self.line = line

        if self.type == UpdateType.INIT:
            self.init_init(line)
        elif self.type == UpdateType.TREE:
            self.init_tree(line)
        elif self.type == UpdateType.TREE_LOGGING:
            self.init_tree_logging(line)
        elif self.type == UpdateType.PAGE_ROLLBACK:
            self.init_page_rollback(line)
        elif self.type == UpdateType.UPDATE_ABORT:
            self.init_update_abort(line)

    def init_init(self, line):
        matches = re.search('stable_timestamp=\((\d+), (\d+)\)', line)
        if matches is None:
            raise Exception("failed to parse init string")
        stable_start = int(matches.group(1))
        stable_stop = int(matches.group(2))
        self.stable = Timestamp(stable_start, stable_stop)

    def init_tree(self, line):
        matches = re.search('file:([\w_\.]+).*modified=(\w+).*durable_timestamp=\((\d+), (\d+)\).*>.*stable_timestamp=\((\d+), (\d+)\): (\w+).*has_prepared_updates=(\w+).*durable_timestamp_not_found=(\w+).*txnid=(\d+).*recovery_checkpoint_snap_min=(\d+): (\w+)', line)
        if matches is None:
            raise Exception("failed to parse tree string")

        self.file = matches.group(1)

        self.modified = matches.group(2).lower() == "true"

        durable_start = int(matches.group(3))
        durable_stop = int(matches.group(4))
        self.durable = Timestamp(durable_start, durable_stop)
        stable_start = int(matches.group(5))
        stable_stop = int(matches.group(6))
        self.stable = Timestamp(stable_start, stable_stop)
        self.durable_gt_stable = matches.group(7).lower() == "true"

        self.has_prepared_updates = matches.group(8).lower() == "true"

        self.durable_ts_not_found = matches.group(9).lower() == "true"

        self.txnid = int(matches.group(10))
        self.recovery_ckpt_snap_min = int(matches.group(11))
        self.txnid_gt_recov_ckpt_snap_min = matches.group(12).lower() == "true"

    def init_tree_logging(self, line):
        # TODO factor out file extraction
        matches = re.search('file:([\w_\.]+).*connection_logging_enabled=(\w+).*btree_logging_enabled=(\w+)', line)
        if matches is None:
            raise Exception("failed to parse tree logging string")

        self.file = matches.group(1)

        self.conn_logging_enabled = matches.group(2).lower() == "true"
        self.btree_logging_enabled = matches.group(3).lower() == "true"

    def init_page_rollback(self, line):
        matches = re.search('file:([\w_\.]+).*addr=(0x[A-Za-z0-9]+).*modified=(\w+)', line)
        if matches is None:
            raise Exception("failed to parse page rollback string")

        self.file = matches.group(1)
        self.addr = int(matches.group(2), 16)
        self.modified = matches.group(3).lower() == "true"

    def init_update_abort(self, line):
        matches = re.search('file:([\w_\.]+).*txnid=(\d+).*txnid_not_visible=(\w+).*stable_timestamp=\((\d+), (\d+)\).*durable_timestamp=\((\d+), (\d+)\): (\w+).*prepare_state=(\w+).*in_progress=(\w+)', line)
        if matches is None:
            raise Exception("failed to parse page rollback string")

        self.file = matches.group(1)

        self.txnid = int(matches.group(2))
        self.txnid_not_visible = matches.group(3).lower() == "true"

        stable_start = int(matches.group(4))
        stable_stop = int(matches.group(5))
        self.stable = Timestamp(stable_start, stable_stop)

        durable_start = int(matches.group(6))
        durable_stop = int(matches.group(7))
        self.durable = Timestamp(durable_start, durable_stop)

        self.stable_lt_durable = matches.group(8).lower() == "true"

        self.prepare_state = PrepareState[matches.group(9)]

        self.in_progress = matches.group(10).lower() == "true"

class Checker:
    def __init__(self):
        self.stable = None
        self.visited_files = set()

    def apply(self, update):
        if update.type == UpdateType.INIT:
            self.apply_check_init(update)
        elif update.type == UpdateType.TREE:
            self.apply_check_tree(update)
        elif update.type == UpdateType.TREE_LOGGING:
            self.apply_check_tree_logging(update)
        elif update.type == UpdateType.PAGE_ROLLBACK:
            self.apply_check_page_rollback(update)
        elif update.type == UpdateType.UPDATE_ABORT:
            self.apply_check_update_abort(update)
        else:
            raise Exception(f"failed to parse {update.line}")

    def apply_check_init(self, update):
        if self.stable is not None:
            raise Exception("restarted RTS?!")
        self.stable = update.stable

    def apply_check_tree(self, update):
        if update.file in self.visited_files:
            raise Exception(f"visited file {update.file} again")
        self.visited_files.add(update.file)
        self.current_file = update.file

        if not(update.modified or
               update.durable_gt_stable or
               update.has_prepared_updates or
               update.durable_ts_not_found or
               update.txnid_gt_recov_ckpt_snap_min):
            raise Exception(f"unnecessary visit to {update.file}")

        if update.durable_gt_stable and not update.durable > update.stable:
            raise Exception(f"incorrect timestamp comparison: thought {update.durable} > {update.stable}, but it isn't")
        if not update.durable_gt_stable and not update.stable >= update.durable:
            raise Exception(f"incorrect timestamp comparison: thought {update.durable} <= {update.stable}, but it isn't")

        if update.durable_ts_not_found and update.durable != Timestamp(0, 0):
            raise Exception("we thought we didn't have a durable timestamp, but we do")

        if update.stable != self.stable:
            raise Exception(f"stable timestamp spuriously changed from {self.stable} to {update.stable} while rolling back {update.file}")

    def apply_check_tree_logging(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

    def apply_check_page_rollback(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

    def apply_check_update_abort(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

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
