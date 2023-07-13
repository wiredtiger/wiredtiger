#!/bin/sh

# Common shell functions shared across dist/ scripts

# Find the commit from develop at which point the current branch diverged.
# rev-list will show all commits that are present on our current branch but not on develop, and 
# the oldest of these is our first commit post-divergence. If this commit exists then 
# we can take its parent. If no such commits exist then we're currently on a commit in the 
# develop branch and can use HEAD instead
last_commit_from_dev() {
    earliest_commit=$(git rev-list HEAD...develop | tail -n 1)

    if [ -z "${earliest_commit}" ]; then
        git rev-parse HEAD
    else
        git rev-parse "${earliest_commit}"~
    fi
}
