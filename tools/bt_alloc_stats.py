#!/usr/bin/env python3

import argparse
import re

from enum import Enum

class OpType(Enum):
    ALLOC_LEAF = 0
    FREE_LEAF = 1
    ALLOC_UPD = 2
    FREE_UPD = 3

class Operation:
    def __init__(self, line):
        self.line = line
        self.type = Operation.__get_type(line)
        self.timestamp = Operation.__get_timestamp(line)

        if self.type == OpType.ALLOC_LEAF:
            self.__init_alloc_leaf(line)
        elif self.type == OpType.FREE_LEAF:
            self.__init_free_leaf(line)
        elif self.type == OpType.ALLOC_UPD:
            self.__init_alloc_upd(line)
        elif self.type == OpType.FREE_UPD:
            self.__init_free_upd(line)
        else:
            raise Exception(f"Operation init got a weird type: {self.type=}")

    @staticmethod
    def __get_type(line):
        if "[ALLOC_LEAF]" in line:
            return OpType.ALLOC_LEAF
        elif "[FREE_LEAF]" in line:
            return OpType.FREE_LEAF
        elif "[ALLOC_UPD]" in line:
            return OpType.ALLOC_UPD
        elif "[FREE_UPD]" in line:
            return OpType.FREE_UPD
        else:
            raise Exception("Couldn't figure out the operation type")

    @staticmethod
    def __get_timestamp(line):
        matches = re.search('^\[(\d+):(\d+)\]', line)

        secs = int(matches.group(1))
        usecs = int(matches.group(2))

        return secs + (usecs / 1000000)

    def __init_alloc_leaf(self, line):
        matches = re.search("tree_id=(\d+)", line)
        self.tree_id = int(matches.group(1))

        matches = re.search("page_addr=0x([A-Za-z0-9]+)", line)
        self.page_addr = int(matches.group(1), 16)

        matches = re.search("entries=(\d+)", line)
        self.entries = int(matches.group(1))

        matches = re.search("sz=(\d+)B", line)
        self.size = int(matches.group(1))

    def __init_free_leaf(self, line):
        matches = re.search("page_addr=0x([A-Za-z0-9]+)", line)
        self.page_addr = int(matches.group(1), 16)

        matches = re.search("freed=(\d+)", line)
        self.freed = int(matches.group(1))

    def __init_alloc_upd(self, line):
        matches = re.search("page_addr=0x([A-Za-z0-9]+)", line)
        self.page_addr = int(matches.group(1), 16)

        matches = re.search("upd_addr=0x([A-Za-z0-9]+)", line)
        self.upd_addr = int(matches.group(1), 16)

        matches = re.search("size=(\d+)", line)
        self.size = int(matches.group(1))

    def __init_free_upd(self, line):
        matches = re.search("page_addr=0x([A-Za-z0-9]+)", line)
        self.page_addr = int(matches.group(1), 16)

        matches = re.search("upd_addr=0x([A-Za-z0-9]+)", line)
        if matches is None:
            self.upd_addr = 0
        else:
            self.upd_addr = int(matches.group(1), 16)

class Summariser:
    def __init__(self):
        self.page_mapping = {}
        self.page_updates = {}

        self.min_page_size_at_creation = 999999999999
        self.max_page_size_at_creation = 0
        self.creation_page_sizes = []

        self.min_entries_at_creation = 999999999999
        self.max_entries_at_creation = 0
        self.creation_entries = []

        self.min_upd_size = 999999999999
        self.max_upd_size = 0
        self.upd_sizes = []

    def apply(self, op):
        if op.type == OpType.ALLOC_LEAF:
            self.__apply_alloc_leaf(op)
        elif op.type == OpType.FREE_LEAF:
            self.__apply_free_leaf(op)
        elif op.type == OpType.ALLOC_UPD:
            self.__apply_alloc_upd(op)
        elif op.type == OpType.FREE_UPD:
            self.__apply_free_upd(op)
        else:
            raise Exception(f"summariser apply got a weird update type: {op.type=}")

    def __apply_alloc_leaf(self, op):
        self.page_mapping[op.page_addr] = op.tree_id
        self.page_updates[op.page_addr] = 0

        if op.size < self.min_page_size_at_creation:
            self.min_page_size_at_creation = op.size
        if op.size > self.max_page_size_at_creation:
            self.max_page_size_at_creation = op.size
        self.creation_page_sizes.append(op.size)

        if op.entries > self.max_entries_at_creation:
            self.max_entries_at_creation = op.entries
        if op.entries < self.min_entries_at_creation:
            self.min_entries_at_creation = op.entries
        self.creation_entries.append(op.entries)

    def __apply_free_leaf(self, op):
        # TODO categorise the frees - count pages remaining
        pass

    def __apply_alloc_upd(self, op):
        self.page_updates[op.page_addr] += 1

        if op.size < self.min_upd_size:
            self.min_upd_size = op.size
        if op.size > self.max_upd_size:
            self.max_upd_size = op.size
        self.upd_sizes.append(op.size)

    def __apply_free_upd(self, op):
        pass

    def summary(self):
        avg_page_size = sum(self.creation_page_sizes) / len(self.creation_page_sizes)
        avg_entries = sum(self.creation_entries) / len(self.creation_entries)
        avg_upd_sz = sum(self.upd_sizes) / len(self.upd_sizes)
        return f"{self.min_page_size_at_creation=}" + \
            f"\n{self.max_page_size_at_creation=}" + \
            f"\n{avg_page_size=}" + \
            f"\n{self.min_entries_at_creation=}" + \
            f"\n{self.max_entries_at_creation=}" + \
            f"\n{avg_entries=}" + \
            f"\n{self.min_upd_size=}" + \
            f"\n{self.max_upd_size=}" + \
            f"\n{avg_upd_sz=}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Report stats for custom B-Tree allocations.")
    parser.add_argument("file", type=str, help="The file to parse stats from.")
    args = parser.parse_args()

    summariser = Summariser()
    with open(args.file) as f:
        for line in f:
            if "WT_VERB_BT_ALLOC" in line:
                op = Operation(line)
                summariser.apply(op)

    print(summariser.summary())
