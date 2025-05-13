import argparse, sys, subprocess, os

def last_commit_from_dev():
    # Find the commit from develop at which point the current branch diverged.
    # rev-list will show all commits that are present on our current branch but not on develop, and
    # the oldest of these is our first commit post-divergence. If this commit exists then
    # we can take its parent. If no such commits exist then we're currently on a commit in the
    # develop branch and can use HEAD instead

    earliest_commit = subprocess.run( "git rev-list HEAD...develop | tail -n 1",
        shell=True, capture_output=True, text=True).stdout

    commit_on_dev = f"{earliest_commit}~" if earliest_commit else "HEAD"

    return subprocess.run(f"git rev-parse {commit_on_dev}",
        shell=True, capture_output=True, text=True).stdout.strip()

def files_changed_from_commit(prefix, commit = None):
    # `git diff --name-only` returns file names relative to the root of the repository.
    # If we want to use these file names from another location, we need to prefix them.
    # NOTE: The prefix must exactly match how the script refers to the repository root!
    # For example, if the script searches for files from the root via `find src -name *.c`,
    # then the prefix must be "".
    # If the script searches for files from the `dist` dir via `find ../ -name *.c`,
    # then the prefix must be "../".
    # Other ways to refer the file names (like "../dist/..") will not work.
    if commit is None:
        commit = last_commit_from_dev()
    # Find the files that have changed since the last commit
    return [prefix+f for f in subprocess.run(f"git diff --name-only {commit}",
        shell=True, capture_output=True, text=True).stdout.strip().splitlines()]

def get_changed_modules():
    modules = set()
    changes = files_changed_from_commit("")

    for change in changes:
        path = os.path.dirname(change)
        if "src/" in path:
            module = os.path.basename(path)
            if module != "..":
                modules.add(module)


    return sorted(modules)

# def list_tests(tags):
#     #List tests based on the tags given

# def run_tests(list):
#     #Run listed tests locally


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test runner based on changed files.")
    parser.add_argument("--list_only", help="Only list the test files without running them.")
    parser.add_argument("-t", help="Run all test relevant to the tag (module) given.")
    args = parser.parse_args()

    changed_modules = get_changed_modules()

    print(changed_modules)

    # temporary
    sys.exit(0)

    changed_files = filter_changed_files()
    tags = get_tags_from_changes(changed_files)
    test_files = list_tests(tags)

    if args.list_only:
        print("\n".join(test_files))
        sys.exit(0)

    for test_file in test_files:
        run_test(test_file)