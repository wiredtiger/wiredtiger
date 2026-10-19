# Atomic review notification POC

One workflow and a small script request review only from `@luke-pearson` for
the live demo. Change `REVIEWER` back to `ivankochin` after the demo.
There is no approval check, merge gate, label or automatic comment.

## Semantics

On PR creation, reopening or new commits:

1. Read the current PR and its complete changed-file list through GitHub's API.
2. Require a notification for any change to `src/include/gcc.h`,
   `src/include/msvc.h` or `src/include/hardware.h`, including renames.
3. Otherwise match atomic, barrier and TSAN interface names in each file's
   patch. Patches include added lines, removed lines and nearby context.
4. If a match exists and Luke has no pending request, request his review.
5. If no match exists and Luke has a pending request, remove it.
6. Otherwise do nothing. Skip Luke-authored PRs: GitHub forbids self-review
   requests.

This scans the whole current PR diff, not only the newest commit. If Luke
already completed a review, a later commit with matching changes requests
review again. Completed reviews are never deleted.

## Deliberate limitations

- Only the three synchronization implementation headers always trigger;
  other headers are scanned like other files.
- Token matching cannot recognize every semantic concurrency change. For
  those changes, request Luke's review manually in GitHub.
- GitHub may omit/truncate patches and caps the file endpoint at 3,000 files.
  This small notification-only POC does not add fallback detection.
- When a diff stops matching, the script removes Luke's pending request even
  if someone originally requested it manually. It does not track ownership.
- It does not inspect approvals or enforce any merge requirement.

## Run locally

```sh
node --test .github/scripts/atomic_review.test.js
```

Tests simulate GitHub calls; they do not send notifications. The live demo
publishes two branches and a PR between them, without changing repository settings.
An Actions demo requires installing the workflow and script on the trusted
base branch. The workflow executes only base-branch code, never PR code.
This branch-to-branch demo uses `pull_request` and supports same-repository
PRs only. When installing on `develop`, use `pull_request_target` for fork
PRs; retain the explicit base-commit checkout with that write-token event.
