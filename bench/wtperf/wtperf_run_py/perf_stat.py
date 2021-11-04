#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from enum import Enum

class AggregationFunc(Enum):
    CoreAverage = 1
    AvgMin = 2
    AvgMax = 3

class PerfStat:
    def __init__(self,
                 short_label: str,
                 pattern: str,
                 input_offset: int,
                 output_label: str,
                 output_precision: int = 0,
                 conversion_function=int,
                 aggregation_function: AggregationFunc = AggregationFunc.CoreAverage):
        self.short_label: str = short_label
        self.pattern: str = pattern
        self.input_offset: int = input_offset
        self.output_label: str = output_label
        self.output_precision: int = output_precision
        self.conversion_function = conversion_function
        self.aggregation_function = aggregation_function
        self.values = []

    def add_values(self, values: list):
        for val in values:
            converted_value = self.conversion_function(val)
            self.values.append(converted_value)

    def get_num_values(self):
        return len(self.values)

    def average(self, vals):
        return self.conversion_function(sum(vals) / len(vals))

    def get_skipminmax_average(self):
        assert len(self.values) >= 3
        drop_min_and_max = sorted(self.values)[1:-1]
        return self.average(drop_min_and_max)

    def get_core_average(self):
        if len(self.values) >= 3:
            return self.get_skipminmax_average()
        else:
            return self.average(self.values)

    def get_avg_min(self):
        min_3_vals = sorted(self.values)[:3]
        return self.average(min_3_vals)

    def get_avg_max(self):
        max_3_vals = sorted(self.values)[-3:]
        return self.average(max_3_vals)

    def are_values_all_zero(self):
        result = True
        for value in self.values:
            if value != 0:
                result = False
        return result
