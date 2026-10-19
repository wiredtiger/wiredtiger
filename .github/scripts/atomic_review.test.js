// Copyright (c) 2026 MongoDB, Inc.
'use strict';

const {test} = require('node:test');
const assert = require('node:assert/strict');
const {needsReview, run} = require('./atomic_review');

test('headers always match; other files match atomics, barriers and TSAN in the patch', () => {
    for (const filename of ['src/include/gcc.h', 'src/include/msvc.h', 'src/include/hardware.h']) {
        assert.equal(needsReview([{filename, patch: '+ordinary change'}]), true);
        assert.equal(needsReview([{filename: 'renamed.h', previous_filename: filename}]), true);
    }
    for (const patch of ['+__wt_atomic_store_uint64_release(p, 1);',
        '-__wt_atomic_cas_uint32(p, 1, 2);', '+WT_FULL_BARRIER();',
        '+__wt_tsan_suppress_load_uint64(p);', '+WT_READ_ONCE(p, v);',
        ' __wt_atomic_store_uint64_release(\n- p,\n+ q,'])
        assert.equal(needsReview([{filename: 'src/txn/txn.c', patch}]), true);
    assert.equal(needsReview([{filename: 'src/txn/txn.c', patch: '+ordinary change'}]), false);
    assert.equal(needsReview([{filename: 'image.png'}]), false);
});

test('request only Luke, avoid duplicate requests, and remove when no longer needed', async () => {
    const calls = [];
    const pr = {state: 'open', user: {login: 'someone-else'}, requested_reviewers: []};
    let files = [{filename: 'src/include/hardware.h'}];
    const github = {
        rest: {pulls: {
            get: async () => ({data: pr}), listFiles: 'files',
            requestReviewers: async args => calls.push(['add', args.reviewers]),
            removeRequestedReviewers: async args => calls.push(['remove', args.reviewers])
        }},
        paginate: async () => files
    };
    const context = {repo: {owner: 'example', repo: 'wiredtiger'}, payload: {pull_request: {number: 42}}};
    const execute = () => run({github, context, core: {info() {}}});
    await execute();
    assert.deepEqual(calls, [['add', ['luke-pearson']]]);
    pr.requested_reviewers = [{login: 'luke-pearson'}];
    await execute();
    assert.equal(calls.length, 1);
    files = [{filename: 'src/txn/txn.c', patch: '+ordinary change'}];
    await execute();
    assert.deepEqual(calls.at(-1), ['remove', ['luke-pearson']]);
    pr.user.login = 'luke-pearson';
    await execute();
    assert.equal(calls.length, 2);
});
