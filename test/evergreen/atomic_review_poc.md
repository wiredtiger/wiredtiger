# Evergreen atomic review notification POC

The `atomic-review-notification` task on its own lightweight variant uses
Evergreen's existing `github.generate_token` command, then runs `gh api` via
`atomic_review.py`. It requests review from
`@wiredtiger/storage-engines-atomics-reviewers` when the current PR diff
contains atomic, barrier or TSAN interfaces, or changes any of the three
synchronization headers (`gcc.h`, `msvc.h`, `hardware.h`). When the diff no
longer matches, it removes the pending team request. No approval or merge
gate is enforced, and completed reviews are untouched.

The App must support organization Members: read and repository Pull requests:
write. The task explicitly requests both, so token-generation or API errors
will expose missing permissions instead of falling back to personal credentials.
The token is used only on the execution host, in GH_TOKEN, with shell logging
disabled. The task installs a pinned gh CLI for either ARM64 or x86-64 Linux.

GitHub PR builds use `github_pr_number`. For a manual patch, set the
`atomic_review_pr_number` parameter to the demo PR number. The task and variant
are tagged `pull_request` for the existing project's PR alias. Confirm that
the project's GitHub PR trigger uses that alias on each update.
Mainline builds without a PR number skip the notification.

The scanner includes added/removed lines and nearby diff context. GitHub's
missing/truncated patches and 3,000-file limit have no fallback in this POC.
Removing a pending request does not distinguish manual requests from automated
ones. The target repository is fixed to wiredtiger/wiredtiger.

Run local tests without sending notifications:

```sh
python3 -m unittest discover -s test/evergreen -p test_atomic_review.py
```

This task follows the existing Evergreen PR-reporting token pattern. Before
production deployment, confirm project restrictions on token access for
untrusted/fork patches and run only approved notification code with credentials.
