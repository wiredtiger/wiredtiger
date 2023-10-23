#!/usr/bin/env python3

import argparse
import re

from enum import Enum

class OpType(Enum):
    ALLOC_LEAF = 0
    FREE_LEAF = 1
    ALLOC_UPD = 2
    FREE_UPD = 3

class Summariser:
    def __init__(self):
        self.page_mapping = None

    def summary(self):
        return "TODO"

class Operation:
    def __init__(self, line):
        self.line = line
        self.type = __get_type(line)
        self.timestamp = __get_timestamp(line)

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
        if "[FREE_LEAF]" in line:
            return OpType.FREE_LEAF
        if "[ALLOC_UPD]" in line:
            return OpType.ALLOC_UPD
        if "[FREE_UPD]" in line:
            return OpType.FREE_UPD

    @staticmethod
    def __get_timestamp(line):
        matches = re.search('^[(\d+):(\d+)]')
        print("1={} 2={}", matches.group(1), matches.group(2))
        return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Report stats for custom B-Tree allocations.")
    parser.add_argument("file", type=str, help="The file to parse stats from.")
    args = parser.parse_args()

    summary = Summariser()
    with open(args.file) as f:
        for line in f:
            if "WT_VERB_BT_ALLOC" in line:
                op = Operation(line)
                summariser.apply(op)

    print(summary.summary())
