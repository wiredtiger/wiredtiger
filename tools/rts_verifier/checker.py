#!/usr/bin/env python3

import update

from basic_types import Timestamp
from update import UpdateType

class Checker:
    def __init__(self):
        self.stable = None
        self.visited_files = set()

    def apply(self, update):
        if update.type == UpdateType.INIT:
            self.__apply_check_init(update)
        elif update.type == UpdateType.TREE:
            self.__apply_check_tree(update)
        elif update.type == UpdateType.TREE_LOGGING:
            self.__apply_check_tree_logging(update)
        elif update.type == UpdateType.PAGE_ROLLBACK:
            self.__apply_check_page_rollback(update)
        elif update.type == UpdateType.UPDATE_ABORT:
            self.__apply_check_update_abort(update)
        else:
            raise Exception(f"failed to parse {update.line}")

    def __apply_check_init(self, update):
        if self.stable is not None:
            raise Exception("restarted RTS?!")
        self.stable = update.stable

    def __apply_check_tree(self, update):
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

    def __apply_check_tree_logging(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

    def __apply_check_page_rollback(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

    def __apply_check_update_abort(self, update):
        if update.file != self.current_file:
            raise Exception(f"spurious visit to {update.file}")

