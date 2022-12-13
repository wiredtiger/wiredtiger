#!/usr/bin/env python3

import re

from basic_types import PrepareState, Timestamp
from enum import Enum

class OpType(Enum):
    INIT = 0
    TREE = 1
    TREE_LOGGING = 2
    PAGE_ROLLBACK = 3
    UPDATE_ABORT = 4
    PAGE_ABORT_CHECK = 5
    UNKNOWN = 6

class Operation:
    def __init__(self, line):
        self.line = line

        if '[INIT]' in line:
            self.__init_init(line)
        elif '[TREE]' in line:
            self.__init_tree(line)
        elif '[TREE_LOGGING]' in line:
            self.__init_tree_logging(line)
        elif '[PAGE_ROLLBACK]' in line:
            self.__init_page_rollback(line)
        elif '[UPDATE_ABORT]' in line:
            self.__init_update_abort(line)
        elif '[PAGE_ABORT_CHECK]' in line:
            self.__init_page_abort_check(line)
        else:
            raise Exception(f"Operation.__init__: couldn't find an event in RTS log line {line}")

    def __repr__(self):
        return f"{self.__dict__}"

    def __init_init(self, line):
        self.type = OpType.INIT

        matches = re.search('stable_timestamp=\((\d+), (\d+)\)', line)
        if matches is None:
            raise Exception("failed to parse init string")

        stable_start = int(matches.group(1))
        stable_stop = int(matches.group(2))
        self.stable = Timestamp(stable_start, stable_stop)

    def __init_tree(self, line):
        self.type = OpType.TREE

        matches = re.search('file:([\w_\.]+)', line)
        self.file = matches.group(1)

        matches = re.search('modified=(\w+)', line)
        self.modified = matches.group(1).lower() == "true"

        matches = re.search('durable_timestamp=\((\d+), (\d+)\).*>.*stable_timestamp=\((\d+), (\d+)\): (\w+)', line)
        durable_start = int(matches.group(1))
        durable_stop = int(matches.group(2))
        self.durable = Timestamp(durable_start, durable_stop)
        stable_start = int(matches.group(3))
        stable_stop = int(matches.group(4))
        self.stable = Timestamp(stable_start, stable_stop)
        self.durable_gt_stable = matches.group(5).lower() == "true"

        matches = re.search('has_prepared_updates=(\w+)', line)
        self.has_prepared_updates = matches.group(1).lower() == "true"

        matches = re.search('durable_timestamp_not_found=(\w+)', line)
        self.durable_ts_not_found = matches.group(1).lower() == "true"

        matches = re.search('txnid=(\d+).*>.*recovery_checkpoint_snap_min=(\d+): (\w+)', line)
        self.txnid = int(matches.group(1))
        self.recovery_ckpt_snap_min = int(matches.group(2))
        self.txnid_gt_recov_ckpt_snap_min = matches.group(3).lower() == "true"

    def __init_tree_logging(self, line):
        self.type = OpType.TREE_LOGGING

        # TODO factor out file extraction
        matches = re.search('file:([\w_\.]+)', line)
        self.file = matches.group(1)

        matches = re.search('connection_logging_enabled=(\w+)', line)
        self.conn_logging_enabled = matches.group(1).lower() == "true"

        matches = re.search('btree_logging_enabled=(\w+)', line)
        self.btree_logging_enabled = matches.group(1).lower() == "true"

    def __init_page_rollback(self, line):
        self.type = OpType.PAGE_ROLLBACK

        matches = re.search('file:([\w_\.]+)', line)
        self.file = matches.group(1)

        matches = re.search('addr=(0x[A-Za-z0-9]+)', line)
        self.addr = int(matches.group(1), 16)

        matches = re.search('modified=(\w+)', line)
        self.modified = matches.group(1).lower() == "true"

    def __init_update_abort(self, line):
        self.type = OpType.UPDATE_ABORT

        matches = re.search('file:([\w_\.]+)', line)
        self.file = matches.group(1)

        matches = re.search('txnid=(\d+)', line)
        self.txnid = int(matches.group(1))
        matches = re.search('txnid_not_visible=(\w+)', line)
        self.txnid_not_visible = matches.group(1).lower() == "true"

        matches = re.search('stable_timestamp=\((\d+), (\d+)\).*<.*durable_timestamp=\((\d+), (\d+)\): (\w+)', line)
        stable_start = int(matches.group(1))
        stable_stop = int(matches.group(2))
        self.stable = Timestamp(stable_start, stable_stop)

        durable_start = int(matches.group(3))
        durable_stop = int(matches.group(4))
        self.durable = Timestamp(durable_start, durable_stop)

        self.stable_lt_durable = matches.group(5).lower() == "true"

        matches = re.search('prepare_state=(\w+)', line)
        self.prepare_state = PrepareState[matches.group(1)]

    def __init_page_abort_check(self, line):
        self.type = OpType.PAGE_ABORT_CHECK

        matches = re.search('file:([\w_\.]+)', line)
        self.file = matches.group(1)

        matches = re.search('ref=(0x[A-Za-z0-9]+)', line)
        self.ref = int(matches.group(1), 16)

        matches = re.search('durable_timestamp=\((\d+), (\d+)\)', line)
        durable_start = int(matches.group(1))
        durable_stop = int(matches.group(2))
        self.durable = Timestamp(durable_start, durable_stop)

        matches = re.search('newest_txn=(\d+)', line)
        self.newest_txn = int(matches.group(1))

        matches = re.search('prepared_updates=(\w+)', line)
        self.has_prepared = matches.group(1).lower() == "true"

        matches = re.search('needs_abort=(\w+)', line)
        self.needs_abort = matches.group(1).lower() == "true"
