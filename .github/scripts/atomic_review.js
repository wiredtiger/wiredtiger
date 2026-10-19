// Copyright (c) 2026 MongoDB, Inc.
'use strict';

const REVIEWER = 'luke-pearson';
const HEADERS = ['src/include/gcc.h', 'src/include/msvc.h', 'src/include/hardware.h'];
const TOKENS = /\b(?:__wt_atomic\w*|__wt_tsan\w*|__tsan_\w*|WT_\w*BARRIER\w*|WT_READ_ONCE|WT_WRITE_ONCE|__atomic_\w*|__sync_\w*|__ATOMIC_\w*|TSAN_BUILD)\b|no_sanitize[^\n]*thread/;

function needsReview(files) {
    return files.some(file => HEADERS.includes(file.filename) ||
        HEADERS.includes(file.previous_filename) || TOKENS.test(file.patch || ''));
}

async function run({github, context, core}) {
    const args = {...context.repo, pull_number: context.payload.pull_request.number};
    const {data: pr} = await github.rest.pulls.get(args);
    // GitHub does not allow requesting review from the PR author.
    if (pr.state !== 'open' || pr.user.login === REVIEWER)
        return;
    const files = await github.paginate(github.rest.pulls.listFiles, {...args, per_page: 100});
    const needed = needsReview(files);
    const requested = pr.requested_reviewers.some(user => user.login === REVIEWER);
    core.info(`Expert review needed: ${needed}; already requested: ${requested}`);
    if (needed && !requested)
        await github.rest.pulls.requestReviewers({...args, reviewers: [REVIEWER]});
    else if (!needed && requested)
        await github.rest.pulls.removeRequestedReviewers({...args, reviewers: [REVIEWER]});
}

module.exports = {needsReview, run};
