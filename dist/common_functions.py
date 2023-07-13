#! /bin/python3

import subprocess

# Common Python functions shared across dist/ scripts

def last_commit_from_dev():
    # Find the commit from develop at which point the current branch diverged.
    # rev-list will show all commits that are present on our current branch but not on develop, and 
    # the oldest of these is our first commit post-divergence. If this commit exists then 
    # we can take its parent. If no such commits exist then we're currently on a commit in the 
    # develop branch and can use HEAD instead

    earliest_commit = subprocess.run( "git rev-list HEAD...develop | tail -n 1", 
        shell=True, capture_output=True, text=True).stdout

    if earliest_commit != "":
        return subprocess.run(f"git rev-parse {earliest_commit}~", 
            shell=True, capture_output=True, text=True).stdout
    else:
        return subprocess.run(f"git rev-parse HEAD", 
            shell=True, capture_output=True, text=True).stdout    
