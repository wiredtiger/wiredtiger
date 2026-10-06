#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

"""Check each random trial's trace and compare its hit rate with recorded provenance."""

import argparse
import collections
import json
import math
import pathlib


parser = argparse.ArgumentParser()
parser.add_argument('batch', type=pathlib.Path)
parser.add_argument('--probability', type=float, help='explicit probability for legacy batches')
args = parser.parse_args()
def require(condition, message):
    if not condition:
        raise AssertionError(message)

report = json.loads((args.batch / 'results.json').read_text())
previous_path = args.batch / 'probability-analysis.json'
previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
provenance = report.get('provenance', {})
probability = args.probability
if probability is None:
    probability = provenance.get('nominal_probability', previous.get('nominal_probability'))
require(probability is not None and 0 < probability < 1,
        'probability unknown: supply --probability for this legacy batch')
if provenance:
    require(args.probability is None or args.probability == provenance['nominal_probability'],
            'requested probability conflicts with captured build provenance')
trials = report['runs']
require(bool(trials), 'empty batch')
hits = 0
for trial in trials:
    require(trial['valid'] and trial['mode'] == 'ci-random', trial)
    events = []
    for line in (pathlib.Path(trial['home']) / 'repro.trace').read_text().splitlines():
        fields = collections.defaultdict(list)
        for part in line.split():
            if '=' in part:
                key, value = part.split('=', 1)
                fields[key].append(value)
        require(not fields['intervention'], (trial['home'], fields['intervention']))
        if (fields['event'] == ['rec-before-wrapup'] and
                fields['stage'] == ['pre-boundary-eviction'] and
                fields['uri'] == ['file:T00002.wt_stable']):
            events.append(fields)
    require(len(events) == 1, (trial['home'], len(events)))
    require(events[0]['detail'] in (['0'], ['16']), 'unexpected reconciliation result')
    hit = events[0]['detail'] == ['16']
    require(hit == trial['builtin_failure'], trial)
    if not report.get('arguments', {}).get('expect_match'):
        require(hit == trial['mismatch'], trial)
    hits += hit

n, p = len(trials), probability
rate, z = hits / n, 1.959963984540054
denominator = 1 + z * z / n
center = (rate + z * z / (2 * n)) / denominator
radius = z * math.sqrt(rate * (1 - rate) / n + z * z / (4 * n * n)) / denominator
probabilities = [math.exp(math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)
                         + k * math.log(p) + (n - k) * math.log1p(-p)) for k in range(n + 1)]
cdf, lower, upper = 0, None, None
for k, mass in enumerate(probabilities):
    cdf += mass
    if lower is None and cdf >= 0.025:
        lower = k
    if upper is None and cdf >= 0.975:
        upper = k
exact_p = sum(mass for mass in probabilities if mass <= probabilities[hits] * (1 + 1e-10))
mismatches = sum(trial['mismatch'] for trial in trials)
summary = dict(trials=n, failpoint_hits=hits, mismatches=mismatches, matching_trials=n - mismatches,
               unexpected_errors=0, observed_rate=rate, nominal_probability=p,
               expected_hits=n * p, binomial_standard_deviation=math.sqrt(n * p * (1 - p)),
               central_95pct_count_range=[lower, upper], wilson_95pct_rate_interval=[
                   center - radius, center + radius], exact_two_sided_binomial_p_value=exact_p,
               consistent_at_5pct=exact_p >= 0.05,
               trace_check='one relevant pre-wrapup outcome per trial; no custom intervention',
               provenance=provenance or {key: previous[key] for key in
                   ('library_sha256', 'rec_write_sha256', 'nominal_probability') if key in previous},
               provenance_origin='batch build manifest' if provenance else
                   'legacy analysis-recorded metadata; not independently authenticated')
output = args.batch / 'probability-reanalysis.json'
output.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
