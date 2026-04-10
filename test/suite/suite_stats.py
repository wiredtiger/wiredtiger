#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.

"""
suite_stats.py — scenario counts and timing aggregates for test/suite/run.py.

Scenario counts (dry-run):
  cd cmake_build && python3 ../test/suite/suite_stats.py scenarios

Timing summary (JSON Lines from --timing-report):
  python3 ../test/suite/suite_stats.py summarize-timing timing.jsonl

Full timing breakdown (slowest scenarios + per-module totals):
  python3 ../test/suite/suite_stats.py analyze-jsonl timing.jsonl --top 40

Timing summary (verbose text from run.py -v 2):
  python3 ../test/suite/suite_stats.py summarize-timing --format text run.log
"""

from __future__ import annotations

import argparse
import collections
import json
import os
from pathlib import Path
import re
import subprocess
import sys

# test_base01.test_base01.test_empty -s 0 (column)
RE_DRY_LINE = re.compile(
    r'^(?P<module>test_\w+)\.(?P<class>\w+)\.(?P<method>\w+)'
    r'(?: -s (?P<snum>\d+) \((?P<sname>[^)]+)\))?$'
)

# [pid:123]: test_base01.test_base01.test_empty -s 0 (column): 0.06 seconds
RE_VERBOSE_TIMING = re.compile(
    r'\[pid:\d+\]: (?P<id>.+): (?P<sec>[\d.]+) seconds'
)


def _run_dry_run(build_dir: str, python_exe: str, fast: bool, long_suite: bool, extra_long: bool):
    run_py = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.py')
    cmd = [python_exe, run_py, '-n']
    if fast:
        cmd.append('--fast')
    if long_suite:
        cmd.append('--long')
    if extra_long:
        cmd.append('--extra-long')
    return subprocess.run(
        cmd,
        cwd=build_dir,
        capture_output=True,
        text=True,
    )


def cmd_scenarios(args: argparse.Namespace) -> int:
    r = _run_dry_run(args.build_dir, args.python, args.fast, args.long, args.extra_long)
    if r.returncode != 0:
        sys.stderr.write(r.stderr or '')
        return r.returncode
    by_module: dict[str, int] = collections.Counter()
    by_class: dict[str, int] = collections.Counter()
    total = 0
    bad = 0
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        m = RE_DRY_LINE.match(line)
        if not m:
            bad += 1
            continue
        total += 1
        mod = m.group('module')
        cls = m.group('class')
        by_module[mod] += 1
        by_class[f'{mod}.{cls}'] += 1

    print(f'total_scenarios\t{total}')
    if bad:
        print(f'unparsed_lines\t{bad}', file=sys.stderr)
    print(f'unique_modules\t{len(by_module)}')
    print(f'unique_classes\t{len(by_class)}')
    print()
    print('rank\tscenarios\tmodule')
    for i, (mod, cnt) in enumerate(by_module.most_common(), start=1):
        print(f'{i}\t{cnt}\t{mod}')
    return 0


def _summarize_jsonl(fp) -> dict[str, dict[str, float | int]]:
    by_module: dict[str, dict[str, float | int]] = collections.defaultdict(
        lambda: {'scenarios': 0, 'seconds': 0.0, 'skipped': 0, 'failed': 0}
    )
    for line in fp:
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        mod = o['module']
        ent = by_module[mod]
        ent['scenarios'] += 1
        ent['seconds'] += float(o['seconds'])
        if o.get('skipped'):
            ent['skipped'] += 1
        if not o.get('passed') and not o.get('skipped'):
            ent['failed'] += 1
    return by_module


def _summarize_verbose_text(fp) -> dict[str, dict[str, float | int]]:
    by_module: dict[str, dict[str, float | int]] = collections.defaultdict(
        lambda: {'scenarios': 0, 'seconds': 0.0, 'skipped': 0, 'failed': 0}
    )
    for line in fp:
        m = RE_VERBOSE_TIMING.search(line)
        if not m:
            continue
        tid = m.group('id').strip()
        sec = float(m.group('sec'))
        mm = RE_DRY_LINE.match(tid)
        mod = mm.group('module') if mm else tid.split('.')[0]
        ent = by_module[mod]
        ent['scenarios'] += 1
        ent['seconds'] += sec
    return by_module


def cmd_analyze_jsonl(args: argparse.Namespace) -> int:
    """Full aggregate report for --timing-report JSONL (per-module sums, slowest scenarios)."""
    path = Path(args.input_path)
    rows = []
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))

    by_module: dict[str, dict[str, float | int | list]] = collections.defaultdict(
        lambda: {'count': 0, 'skipped': 0, 'failed': 0, 'sec_sum': 0.0, 'sec_max': 0.0, 'times': []}
    )
    slowest: list[tuple[float, str, str, str | None, bool]] = []

    for r in rows:
        mod = r['module']
        sec = float(r['seconds'])
        ent = by_module[mod]
        ent['count'] += 1
        ent['sec_sum'] += sec
        ent['sec_max'] = max(float(ent['sec_max']), sec)
        ent['times'].append(sec)
        if r.get('skipped'):
            ent['skipped'] += 1
        elif not r.get('passed'):
            ent['failed'] += 1
        slowest.append((sec, mod, r['method'], r.get('scenario_name'), bool(r.get('skipped'))))

    slowest.sort(reverse=True)
    total_sec = sum(float(r['seconds']) for r in rows)
    n_exec = sum(1 for r in rows if not r.get('skipped'))
    n_skip = sum(1 for r in rows if r.get('skipped'))
    n_fail = sum(1 for r in rows if not r.get('passed') and not r.get('skipped'))

    top_n = args.top
    print(f'timing_records\t{len(rows)}')
    print(f'sum_recorded_seconds\t{total_sec:.3f}')
    print(f'executed_scenarios\t{n_exec}')
    print(f'skipped_scenarios\t{n_skip}')
    print(f'failed_executed\t{n_fail}')
    print()
    print(f'## Top {top_n} modules by total recorded seconds (sum over scenarios; parallel run inflates vs wall clock)')
    for i, (mod, d) in enumerate(
        sorted(by_module.items(), key=lambda x: float(x[1]['sec_sum']), reverse=True)[:top_n],
        start=1,
    ):
        times_sorted = sorted(d['times'])
        med = times_sorted[len(times_sorted) // 2]
        print(
            f'{i}\t{float(d["sec_sum"]):.1f}s\tn={int(d["count"])}\t'
            f'max={float(d["sec_max"]):.2f}s\tmed={med:.3f}s\t{mod}'
        )
    print()
    print(f'## Top {top_n} slowest individual scenarios')
    for sec, mod, meth, scen, sk in slowest[:top_n]:
        sn = scen or '-'
        print(f'{sec:8.2f}s\t{"skip" if sk else "run"}\t{mod}.{meth}\t({sn})')
    print()
    print(f'## Top {top_n} modules by max single-scenario time')
    for i, (mod, d) in enumerate(
        sorted(by_module.items(), key=lambda x: float(x[1]['sec_max']), reverse=True)[:top_n],
        start=1,
    ):
        print(f'{i}\tmax={float(d["sec_max"]):.2f}s\tn={int(d["count"])}\t{mod}')
    return 0


def cmd_summarize_timing(args: argparse.Namespace) -> int:
    if args.format == 'jsonl':
        opener = lambda: open(args.input_path, encoding='utf-8')
    else:
        opener = lambda: open(args.input_path, encoding='utf-8', errors='replace')

    if args.input_path == '-':
        by_module = _summarize_jsonl(sys.stdin) if args.format == 'jsonl' else _summarize_verbose_text(sys.stdin)
    else:
        with opener() as fp:
            by_module = _summarize_jsonl(fp) if args.format == 'jsonl' else _summarize_verbose_text(fp)

    rows = sorted(
        by_module.items(),
        key=lambda x: float(x[1]['seconds']),
        reverse=True,
    )
    grand = sum(float(v['seconds']) for _, v in rows)
    print(f'total_wall_sum_modules\t{grand:.3f}')
    print()
    print('rank\tseconds\tscenarios\tmodule')
    for i, (mod, ent) in enumerate(rows, start=1):
        print(
            f'{i}\t{float(ent["seconds"]):.3f}\t{int(ent["scenarios"])}\t{mod}'
        )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description='WiredTiger Python suite statistics')
    sub = p.add_subparsers(dest='cmd', required=True)

    ps = sub.add_parser('scenarios', help='Count scenarios via run.py -n (run from build dir default .)')
    ps.add_argument(
        '-b', '--build-dir', default='.', help='WiredTiger CMake build directory containing wt'
    )
    ps.add_argument('--python', default=sys.executable, help='Python interpreter')
    ps.add_argument('--fast', action='store_true', help='Pass --fast to run.py')
    ps.add_argument('--long', action='store_true', help='Pass --long to run.py')
    ps.add_argument('--extra-long', action='store_true', help='Pass --extra-long to run.py')
    ps.set_defaults(func=cmd_scenarios)

    pt = sub.add_parser(
        'summarize-timing',
        help='Aggregate timing by module (JSONL from --timing-report or verbose log)',
    )
    pt.add_argument(
        'input_path',
        nargs='?',
        default='-',
        help='JSONL file, verbose log, or - for stdin',
    )
    pt.add_argument(
        '--format',
        choices=('jsonl', 'text'),
        default='jsonl',
        help='jsonl: output of run.py --timing-report; text: run.py -v 2 log',
    )
    pt.set_defaults(func=cmd_summarize_timing)

    pa = sub.add_parser(
        'analyze-jsonl',
        help='Full report from run.py --timing-report output (per-module totals and slowest scenarios)',
    )
    pa.add_argument(
        'input_path',
        nargs='?',
        default='wt_timing_full.jsonl',
        help='Path to JSONL (default: wt_timing_full.jsonl in cwd)',
    )
    pa.add_argument('--top', type=int, default=30, help='How many rows per section')
    pa.set_defaults(func=cmd_analyze_jsonl)

    args = p.parse_args()
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
