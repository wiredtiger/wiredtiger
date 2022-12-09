#!/usr/bin/env python3

from basic_types import Page, Timestamp, Tree, Update, UpdateType
from operation import OpType

class Checker:
    def __init__(self):
        self.stable = None
        self.visited_trees = set()
        self.visited_pages = set()

    def apply(self, operation):
        if operation.type == OpType.INIT:
            self.__apply_check_init(operation)
        elif operation.type == OpType.TREE:
            self.__apply_check_tree(operation)
        elif operation.type == OpType.TREE_LOGGING:
            self.__apply_check_tree_logging(operation)
        elif operation.type == OpType.PAGE_ROLLBACK:
            self.__apply_check_page_rollback(operation)
        elif operation.type == OpType.UPDATE_ABORT:
            self.__apply_check_operation_abort(operation)
        else:
            raise Exception(f"failed to parse {operation.line}")

    def __apply_check_init(self, operation):
        if self.stable is not None:
            raise Exception("restarted RTS?!")
        self.stable = operation.stable

    def __apply_check_tree(self, operation):
        tree = Tree(operation.file)
        if tree in self.visited_trees:
            raise Exception(f"visited file {operation.file} again")
        self.visited_trees.add(tree)
        self.current_tree = tree

        if not(operation.modified or
               operation.durable_gt_stable or
               operation.has_prepared_operations or
               operation.durable_ts_not_found or
               operation.txnid_gt_recov_ckpt_snap_min):
            raise Exception(f"unnecessary visit to {operation.file}")

        if operation.durable_gt_stable and not operation.durable > operation.stable:
            raise Exception(f"incorrect timestamp comparison: thought {operation.durable} > {operation.stable}, but it isn't")
        if not operation.durable_gt_stable and not operation.stable >= operation.durable:
            raise Exception(f"incorrect timestamp comparison: thought {operation.durable} <= {operation.stable}, but it isn't")

        if operation.durable_ts_not_found and operation.durable != Timestamp(0, 0):
            raise Exception("we thought we didn't have a durable timestamp, but we do")

        if operation.stable != self.stable:
            raise Exception(f"stable timestamp spuriously changed from {self.stable} to {operation.stable} while rolling back {operation.file}")

    def __apply_check_tree_logging(self, operation):
        if operation.file != self.current_tree.file:
            raise Exception(f"spurious visit to {operation.file}")

        if self.current_tree.logged is not None and self.current_tree.logged != operation.btree_logging_enabled:
            raise Exception(f"{operation.file} spuriously changed btree logging state")

        self.current_tree.logged = operation.btree_logging_enabled

    def __apply_check_page_rollback(self, operation):
        if operation.file != self.current_tree.file:
            raise Exception(f"spurious visit to {operation.file}")

        page = Page(operation.addr)
        page.modified = operation.modified
        if page in self.visited_pages:
            raise Exception(f"visited page {operation.addr} again")
        self.visited_pages.add(page)

    def __apply_check_operation_abort(self, operation):
        update = Update(UpdateType.ABORT, operation.txnid)
        if operation.file != self.current_tree.file:
            raise Exception(f"spurious visit to {operation.file}")

        if not(operation.txnid_not_visible or
               operation.stable_lt_durable or
               operation.prepare_state == PrepareState.IN_PROGRESS):
            raise Exception(f"aborted update {update} for no reason")
