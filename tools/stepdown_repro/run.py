#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

"""Run isolated homes and verify the entire reproducer, including checkpoint views."""

import argparse
import collections
import concurrent.futures
import hashlib
import json
import pathlib
import subprocess
import time


ROOT = pathlib.Path(__file__).resolve().parents[2]
MODES = ('fail', 'no-fail', 'mirror-off', 'no-skip', 'urgent', 'ci-random')


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def load_view(path, normalize=True):
    rows = {}
    previous = None
    for line in path.read_text().splitlines():
        key_hex, value_hex = line.split('\t')
        key, value = bytes.fromhex(key_hex), bytes.fromhex(value_hex)
        require(previous is None or previous < key, f'{path}: unordered or duplicate key')
        previous = key
        # Format mirrors row identities across table-specific suffixes and key lengths.
        identity = key.split(b'/', 1)[0] if normalize else key
        if identity == b'0000552900.03':
            identity = b'0000552900.01'
        require(identity not in rows, f'{path}: duplicate normalized identity')
        rows[identity] = value
    return rows


def validate(home, mode, returncode, expect_match=False):
    text = (home / 'console.log').read_text()
    require(returncode in (0, 2), f'exit={returncode}\n{text}')
    mismatch = 'mismatch=1' in text
    require(mismatch == (returncode == 2), 'RESULT and exit status disagree')
    if mode in ('fail', 'urgent'):
        require(mismatch != expect_match, 'outcome differs from the experiment expectation')
    if expect_match:
        require(not mismatch, 'candidate fix still mismatched')
    if mode in ('no-fail', 'mirror-off', 'no-skip'):
        require(not mismatch, 'preventative control mismatched')
    require('[ERROR]' not in text and 'Sanitizer' not in text, text)

    records = []
    for line in (home / 'repro.trace').read_text().splitlines():
        fields = collections.defaultdict(list)
        for part in line.split():
            if '=' in part:
                key, value = part.split('=', 1)
                fields[key].append(value)
        require(fields['seq'] == [str(len(records) + 1)], 'non-sequential trace')
        records.append(fields)

    def events(event, stage=None):
        return [r for r in records if r['event'] == [event] and
                (stage is None or r['stage'] == [stage])]

    require(records[-1]['stage'] == ['closed'], 'driver did not finish cleanup')
    failed = [r for r in events('rec-before-wrapup')
              if r['intervention'] == ['fail-before-wrapup']]
    if mode != 'ci-random':
        require(len(failed) == (0 if mode == 'no-fail' else 1), 'wrong injection count')
    builtin = False
    if mode == 'ci-random':
        require(not any(r['intervention'] for r in records), 'unexpected custom intervention')
        outcomes = [r for r in events('rec-before-wrapup', 'pre-boundary-eviction')
                    if r['uri'] == ['file:T00002.wt_stable']]
        require(len(outcomes) == 1 and outcomes[0]['detail'] in (['0'], ['16']),
                'expected one relevant random reconciliation outcome')
        builtin = outcomes[0]['detail'] == ['16']
        if not expect_match:
            require(builtin == mismatch, 'random pre-wrapup failure and mismatch disagree')
        opportunities = [r for r in events('rec-failpoint-opportunity', 'pre-boundary-eviction')
                         if r['uri'] == ['file:T00002.wt_stable']]
        # Older retained batches predate this explicit opportunity/RNG event.
        if opportunities:
            require(len(opportunities) == 1, 'wrong number of random opportunities')

    completed = events('checkpoint-complete')
    for pickup in events('pickup-completed'):
        if completed and completed[0]['completed_meta']:
            published = [r for r in completed if int(r['seq'][0]) < int(pickup['seq'][0])]
            require(bool(published), 'pickup preceded every checkpoint publication')
            metadata = dict(pair.split('=', 1) for pair in published[-1]['completed_meta'][0].split(','))
            require(pickup['detail'] == [metadata['metadata_lsn']],
                    'pickup LSN does not match the last published checkpoint')

    bulk = b'0000552900/' + bytes(b'LMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ'[i % 26]
                                 for i in range(1442 - 11))
    neighbor = bulk[:177]
    initial = {b'0000552898.00': neighbor, b'0000552900.00': bulk,
               b'0000552901.00': neighbor}
    final_base = {b'0000552898.00': neighbor, b'0000552900.01': neighbor,
                  b'0000552901.00': bulk}
    final_layered = dict(final_base)
    if mismatch:
        final_layered[b'0000552900.00'] = bulk
    raw_keys = {
        'base': {b'0000552898.00': b'0000552898.00/opqrstuvwxyzabcd',
                 b'0000552900.00': b'0000552900.00/opqrstuvwxyzabcd',
                 b'0000552900.01': b'0000552900.01/opqrstuvwxyzabcd',
                 b'0000552901.00': b'0000552901.00/opqrstuv'},
        'layered': {b'0000552898.00': b'0000552898.00/opqrstuvwxyza',
                    b'0000552900.00': b'0000552900.00/opqrstuvwxyza',
                    b'0000552900.01': b'0000552900.03/opqrstuvwxyza',
                    b'0000552901.00': b'0000552901.00/opqrstuvwxyza'}}

    def view(stage, name, expected):
        table = 'base' if name == 'base' else 'layered'
        raw_expected = {raw_keys[table][k]: value for k, value in expected.items()}
        path = home / f'{stage}-{name}.tsv'
        require(load_view(path, normalize=False) == raw_expected, f'{path}: wrong raw keys/values')
        return expected

    for name in ('base', 'layered'):
        view('reopen-leader', name, initial)
    views = {}
    for stage in ('follower', 'stepup', 'persisted'):
        b = view(stage, 'base', final_base)
        l = view(stage, 'layered', final_layered)
        views[stage] = {'base_rows': len(b), 'layered_rows': len(l),
                        'extra_layered_keys': [k.decode() for k in l.keys() - b.keys()]}
    for stage in ('stepup', 'persisted'):
        view(stage, 'stable-checkpoint', final_layered)
    cutoff_view = dict(final_layered)
    del cutoff_view[b'0000552900.01']
    view('stepdown', 'stable-checkpoint', cutoff_view)
    notfound = str((1 << 64) - 31803)
    for stage in ('reopen-leader', 'follower', 'stepup', 'persisted'):
        for name in ('base', 'layered'):
            expected = '0' if stage == 'reopen-leader' or (name == 'layered' and mismatch) else notfound
            lookup = events(f'logical-{name}', stage)
            require(len(lookup) == 1 and lookup[0]['detail'] == [expected],
                    f'{stage}: unexpected {name} lookup status')

    if mismatch:
        reuse = events('reuse-address', 'stepdown-checkpoint')
        skip = [r for r in events('checkpoint-skip', 'stepdown-checkpoint')
                if r['uri'] == ['file:T00002.wt_stable']]
        require(reuse and skip, 'missing address-reuse/checkpoint-skip evidence')
        require(reuse[0]['newer'] == ['0'] and reuse[0]['removed'] == ['1'],
                'wrong reconciliation change tracking')
        require(reuse[0]['page_lsn'] == skip[0]['page_lsn'], 'skip reused different LSN')
        require(reuse[0]['ref_cookie'] == skip[0]['ref_cookie'], 'skip reused different cookie')
        require(bool(reuse[0]['ref_cookie']), 'missing retained cookie')
        for stage in ('follower', 'stepup', 'persisted'):
            require(len(events('logical-layered', stage)) == 1 and
                    events('logical-layered', stage)[0]['detail'] == ['0'],
                    f'{stage}: resurrected victim not found')
    return dict(mismatch=mismatch, events=len(records), views=views, builtin_failure=builtin)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=20)
    parser.add_argument('--modes', default=','.join(MODES[:-1]))
    parser.add_argument('--jobs', type=int, default=1)
    parser.add_argument('--build', default='build')
    parser.add_argument('--binary', default='tools/stepdown_repro/stepdown-repro')
    parser.add_argument('--name', required=True)
    parser.add_argument('--timeout', type=float, default=60)
    parser.add_argument('--quiet', action='store_true')
    parser.add_argument('--expect-match', action='store_true',
                        help='validate a candidate fix under the original failure schedule')
    args = parser.parse_args()
    modes = args.modes.split(',')
    if len(modes) != len(set(modes)):
        parser.error('modes must be unique')
    if (not args.name or args.name in ('.', '..') or '/' in args.name or '\\' in args.name or
            pathlib.Path(args.name).name != args.name):
        parser.error('name must be a single directory name without parent traversal')
    if args.count < 1 or args.jobs < 1 or args.timeout <= 0 or any(m not in MODES for m in modes):
        parser.error('count/jobs/timeout must be positive and modes must be supported')
    dest = ROOT / 'tools/stepdown_repro' / args.name
    binary, build = (ROOT / args.binary).resolve(), (ROOT / args.build).resolve()
    require(binary.is_file(), f'missing binary: {binary}')
    provenance = json.loads(binary.with_name(binary.name + '.build.json').read_text())
    for key, path in (('binary_sha256', binary), ('library_sha256', build / 'libwiredtiger.so'),
                      ('palite_sha256', build / 'ext/page_log/palite/libwiredtiger_palite.so'),
                      ('config_sha256', build / 'config/wiredtiger_config.h'),
                      ('rec_write_sha256', ROOT / 'src/reconcile/rec_write.c')):
        require(hashlib.sha256(path.read_bytes()).hexdigest() == provenance[key],
                f'{path} changed since driver compilation; rebuild engine and driver')
    require(provenance['build'] == str(build), 'driver build and selected build differ')
    dest.mkdir()
    (dest / 'manifest.json').write_text(json.dumps(provenance, indent=2) + '\n')
    jobs = [(mode, i) for mode in modes for i in range(args.count)]

    def run(job):
        mode, i = job
        home = dest / f'{mode}-{i:04d}'
        home.mkdir()
        start = time.monotonic()
        result = dict(mode=mode, index=i, home=str(home), valid=False)
        try:
            with (home / 'console.log').open('w') as log:
                p = subprocess.run([str(binary), str(home), str(build), mode],
                                   stdout=log, stderr=subprocess.STDOUT,
                                   cwd=ROOT, timeout=args.timeout)
            result['returncode'] = p.returncode
            result.update(validate(home, mode, p.returncode, args.expect_match))
            if mode == 'ci-random':
                opportunities = [line for line in (home / 'repro.trace').read_text().splitlines()
                    if 'event=rec-failpoint-opportunity ' in line and
                    'stage=pre-boundary-eviction ' in line and
                    'uri=file:T00002.wt_stable ' in line]
                require(len(opportunities) == 1, 'missing explicit failpoint opportunity')
                fields = dict(part.split('=', 1) for part in opportunities[0].split() if '=' in part)
                require(int(fields['detail']) == provenance['failpoint_threshold'],
                        'compiled engine probability differs from build manifest')
                result['rng_state_before_draw'] = fields['rng_state']
            result['valid'] = True
        except Exception as error:
            result['error'] = str(error)
        result['seconds'] = round(time.monotonic() - start, 3)
        (home / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
        if not args.quiet or not result['valid']:
            print(json.dumps(result), flush=True)
        return result

    results = []
    with (dest / 'results.jsonl').open('w') as progress:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = [pool.submit(run, job) for job in jobs]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
                progress.write(json.dumps(result) + '\n')
                progress.flush()
    summary = {}
    for result in results:
        group = summary.setdefault(result['mode'], dict(runs=0, mismatches=0, errors=0,
                                                        builtin_failures=0))
        group['runs'] += 1
        group['mismatches'] += result.get('mismatch', False)
        group['errors'] += not result['valid']
        group['builtin_failures'] += result.get('builtin_failure', False)
    report = dict(summary=summary, runs=sorted(results, key=lambda r: (r['mode'], r['index'])),
                  binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  build=str(build), arguments=vars(args), provenance=provenance)
    for key, path in (('binary_sha256', binary), ('library_sha256', build / 'libwiredtiger.so'),
                      ('palite_sha256', build / 'ext/page_log/palite/libwiredtiger_palite.so'),
                      ('config_sha256', build / 'config/wiredtiger_config.h'),
                      ('rec_write_sha256', ROOT / 'src/reconcile/rec_write.c')):
        require(hashlib.sha256(path.read_bytes()).hexdigest() == provenance[key],
                f'{path} changed during the batch; results must not be treated as one build')
    (dest / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print('SUMMARY', json.dumps(summary), flush=True)
    return int(any(not r['valid'] for r in results))


if __name__ == '__main__':
    raise SystemExit(main())
