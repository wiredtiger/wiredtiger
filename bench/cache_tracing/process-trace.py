#!/usr/bin/env python3

import re, sys
from collections import defaultdict
import math

cache_sample_re = re.compile(r'cache-sample page ([^ ]+) addr \[(\d+): (\d+)-.*?\] type leaf read_gen ([^ ]+)')

last_access = {}
frequency_count = defaultdict(int)
count = 0
for line in sys.stdin:
    m = cache_sample_re.search(line)
    if not m:
        continue
    count += 1
    key = m.group(3)
    if key in last_access:
        access_gap = count - last_access[key]
        #print(f'{access_gap}')
        freq = int(math.floor(math.log2(access_gap)))
        frequency_count[freq] += 1
    last_access[key] = count

for freq, value in sorted(frequency_count.items()):
    print(f'{freq}, {value}')

