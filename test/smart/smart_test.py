import argparse, sys, subprocess, os
import json

sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'py_utility'))
import test_util

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

def run_tests(component):

    # FIXME - Handle the case when we don't have tests for the component
    # e.g. what if we don't have any python tests for the component?

    print("==========================")
    print(f"Running python tests for {component}")
    print("==========================")
    # FIXME - the traces being captured on evergreen are only the stderr, so the text is hard to read. Think about improving this
    res = subprocess.run(f"python3 ../suite/run.py {component}",shell=True, cwd=os.curdir)
    if res.returncode != 0:
        print("Error!")
        sys.exit(res.returncode)

    print("==========================")
    print(f"Running catch2 tests for {component}")
    print("==========================")
    wt_builddir = test_util.find_build_dir()
    print(f"path == {wt_builddir}")
    print(f"pwd = {os.path.abspath(os.curdir)}")
    res = subprocess.run(f"{wt_builddir}/test/catch2/catch2-unittests [{component}]", shell=True)

    if res.returncode != 0:
        print("Error!")
        sys.exit(res.returncode)

    # TODO - Make this testing smarter:
    #  - Add tests other than python/catch2. ctest with tags is a good candidate
    #  - Think about runtime. We want PR testing to stay under 30 minutes from start to finish
    #  - Do we want to get even smarted in the long term? i.e. only running a subset of tests based on the coverage data

# Build the smart_test.json file used to generate tests in evergreen
def build_evergreen_generate_file(changed_modules, buildvariant):

    my_json = {
        "tasks": [],  # Filled in the loop below
        "buildvariants": [
            {
                "tasks": [],  # Filled in the loop below
                "name": buildvariant
            }
        ]
    }

    for component in changed_modules:
        task_json = {
                "commands": [
                    # FIXME - this compiles wiredtiger for each test since we need the HAVE_UNITTEST flag. We should look into fetching artifacts instead.
                    {"func": "get project"},
                    {
                        "func": "compile wiredtiger",
                        "vars": { "HAVE_UNITTEST": "-DHAVE_UNITTEST=1" }
                    },
                    {
                        "func": "smart test component",
                        "vars": { "component": component }
                    }
                ],
                "name": f"run_smart_{component}"
            }
        my_json["tasks"].append(task_json)

        my_json["buildvariants"][0]["tasks"].append({ "name": f"run_smart_{component}" })

    json.dump(my_json, open("smart_test.json", "w"), indent=4)

if __name__ == "__main__":

    # Always run this script from its containing directory. We have some
    # hardcoded paths that require it.
    working_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(working_dir)

    parser = argparse.ArgumentParser(description="Test runner based on changed files.")
    parser.add_argument("--list_only", action="store_true", help="Only list the test files without running them.")
    # This takes the buildvariant as an argument
    # FIXME - determine if we can extract the buildvariant from the environment. It's an evergreen expansion.
    parser.add_argument("--generate-tests", help="Generate a smart_test.json file to be used by generate.tasks in evergreen.")
    parser.add_argument("--test-component", help="Test a specific component")
    parser.add_argument("-t", help="Run all test relevant to the tag (module) given.")
    args = parser.parse_args()

    changed_modules = get_changed_modules()

    if args.list_only:
        if changed_modules:
            print(changed_modules)
        else:
            print("No changes made in src/")
        sys.exit(0)
    elif args.generate_tests:
        # Generate the smart_test.json file
        build_evergreen_generate_file(changed_modules, args.generate_tests)
        print("smart_test.json file generated. Pass it into generate.tasks in evergreen to generate dedicated tests for each modified component.")
        sys.exit(0)
    elif args.test_component:
        if args.test_component == "ALL":
            # Run tests for all components with changed files
            for component in changed_modules:
                run_tests(component)
        else:
            # Run the tests for the specific component
            run_tests(args.test_component)
        sys.exit(0)