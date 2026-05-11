#!/usr/bin/env python3
"""Lightweight test for PerfStatLatencyPercentile. Run with: python3 test_perf_stat_percentile.py"""
import os
import tempfile
import sys

# Allow import from the same directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from perf_stat import PerfStatLatencyPercentile

# A fixture latency.update CSV mirroring wtperf's format.
# 1000 ops total: 500 at <=1us, 400 at <=2us, 50 at <=10us, 40 at <=100us, 10 at <=1000us.
FIXTURE = (
    "#usecs,operations,cumulative-operations,total-operations\n"
    "1,500,500,1000\n"
    "2,400,900,1000\n"
    "10,50,950,1000\n"
    "100,40,990,1000\n"
    "1000,10,1000,1000\n"
)

def write_fixture(tmpdir, filename, content):
    p = os.path.join(tmpdir, filename)
    with open(p, 'w') as f:
        f.write(content)
    return p

def main():
    with tempfile.TemporaryDirectory() as d:
        write_fixture(d, "latency.update", FIXTURE)
        cases = [
            (50.0, 1),     # median is in the 1us bucket
            (95.0, 10),    # p95 falls in the 10us bucket
            (99.0, 100),   # p99 falls in the 100us bucket
            (99.9, 1000),  # p99.9 falls in the 1000us bucket
        ]
        for pct, expected in cases:
            stat = PerfStatLatencyPercentile(
                short_label=f"p{pct}",
                stat_file="latency.update",
                output_label=f"Update p{pct} latency us",
                percentile=pct,
                op_name="update",
            )
            values = stat.find_stat(os.path.join(d, "latency.update"))
            assert values, f"no value extracted for p{pct}"
            assert values[0] == expected, \
                f"p{pct}: expected {expected}, got {values[0]}"
            print(f"  p{pct}: {values[0]} us OK")
    print("PASS")

if __name__ == "__main__":
    main()
