#!/usr/bin/env python3

import re

from basic_types import PrepareState, Timestamp
from enum import Enum

class UpdateType(Enum):
    INIT = 0
    TREE = 1
    TREE_LOGGING = 2
    PAGE_ROLLBACK = 3
    UPDATE_ABORT = 4
    UNKNOWN = 5

class Update:
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
        else:
            raise Exception("Update.__init__: couldn't find an event in an RTS log line")

    def __init_init(self, line):
        self.type = UpdateType.INIT

        matches = re.search('stable_timestamp=\((\d+), (\d+)\)', line)
        if matches is None:
            raise Exception("failed to parse init string")

        stable_start = int(matches.group(1))
        stable_stop = int(matches.group(2))
        self.stable = Timestamp(stable_start, stable_stop)

    def __init_tree(self, line):
        self.type = UpdateType.TREE

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

    def __init_tree_logging(self, line):
        self.type = UpdateType.TREE_LOGGING

        # TODO factor out file extraction
        matches = re.search('file:([\w_\.]+).*connection_logging_enabled=(\w+).*btree_logging_enabled=(\w+)', line)
        if matches is None:
            raise Exception("failed to parse tree logging string")

        self.file = matches.group(1)

        self.conn_logging_enabled = matches.group(2).lower() == "true"
        self.btree_logging_enabled = matches.group(3).lower() == "true"

    def __init_page_rollback(self, line):
        self.type = UpdateType.PAGE_ROLLBACK

        matches = re.search('file:([\w_\.]+).*addr=(0x[A-Za-z0-9]+).*modified=(\w+)', line)
        if matches is None:
            raise Exception("failed to parse page rollback string")

        self.file = matches.group(1)
        self.addr = int(matches.group(2), 16)
        self.modified = matches.group(3).lower() == "true"

    def __init_update_abort(self, line):
        self.type = UpdateType.UPDATE_ABORT

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
