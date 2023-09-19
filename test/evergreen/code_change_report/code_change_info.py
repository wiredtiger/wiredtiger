import argparse
import json
from pygit2 import discover_repository, Repository
from pygit2 import GIT_SORT_TOPOLOGICAL, GIT_SORT_REVERSE, GIT_SORT_NONE

from change_info import ChangeInfo
from commit_info import CommitInfo


# This function reads a gcovr json file into a dict and returns it
def read_coverage_data(coverage_data_path: str):
    with open(coverage_data_path) as json_file:
        data = json.load(json_file)
        return data


def find_function(function_list, file_path: str, line_number: int):
    for function in function_list:
        if function['file'] == file_path:
            function_start_line: int = int(function['line start'])
            function_end_line: int = int(function['line end'])
            if function_start_line <= line_number < function_end_line:
                return function
    return None


def get_git_info(git_working_tree_dir: str, verbose: bool):
    repository_path = discover_repository(git_working_tree_dir)
    assert repository_path is not None

    repo = Repository(repository_path)
    latest_commit = repo.head.target
    commits = list(repo.walk(latest_commit, GIT_SORT_NONE))

    if verbose:
        print("Num commits found:{}".format(len(commits)))

    commit = commits[0]
    change_list = dict()

    message_lines = commit.message.splitlines()
    message_first_line = message_lines[0]

    if verbose:
        print("{}: {}".format(commit.hex, message_first_line))

    commit_url = 'https://github.com/wiredtiger/wiredtiger/commit/{}'.format(commit.id)

    if verbose:
        print("  Files changed in {} ({})".format(commit.short_id, commit_url))

    commit_info = CommitInfo(short_id=commit.short_id, long_id=str(commit.id), message=commit.message)
    prev_commit = commit.parents[0]
    diff = prev_commit.tree.diff_to_tree(commit.tree)
    for patch in diff:
        if verbose:
            print('    {}: {}'.format(patch.delta.status_char(), patch.delta.new_file.path))
            if patch.delta.new_file.path != patch.delta.old_file.path:
                print('      (was {})'.format(patch.delta.old_file.path))

        hunks = patch.hunks
        hunk_list = list()
        for hunk in hunks:
            if verbose:
                print('      Hunk:')
                print('        old_start: {}, old_lines: {}'.format(hunk.old_start, hunk.old_lines))
                print('        new_start: {}, new_lines: {}'.format(hunk.new_start, hunk.new_lines))

            change = ChangeInfo(status=patch.delta.status_char(),
                                new_file_path=patch.delta.new_file.path,
                                old_file_path=patch.delta.old_file.path,
                                new_start=hunk.new_start,
                                new_lines=hunk.new_lines,
                                old_start=hunk.old_start,
                                old_lines=hunk.old_lines,
                                lines=hunk.lines)
            hunk_list.append(change)

        change_list[patch.delta.new_file.path] = hunk_list

    return change_list


def find_file_in_coverage_data(coverage_data: dict, file_path: str):
    file_data = None

    for file in coverage_data['files']:
        if file['file'] == file_path:
            file_data = file

    return file_data


def find_covered_branches(coverage_data: dict, file_path: str, line_number: int):
    branches = dict()

    file_data = find_file_in_coverage_data(coverage_data=coverage_data, file_path=file_path)

    if file_data is not None:
        for line_info in file_data['lines']:
            if line_info['line_number'] == line_number:
                branches = line_info['branches']

    return branches


def create_report_info(patch_info: dict, coverage_data: dict):
    report = dict()

    for new_file in patch_info:
        this_patch = patch_info[new_file]
        change_info_list = list()
        for hunk in this_patch:
            change_info = dict()
            change_info['status'] = hunk.status
            change_info['new_start'] = hunk.new_start
            change_info['new_lines'] = hunk.new_lines
            change_info['old_start'] = hunk.old_start
            change_info['old_lines'] = hunk.old_lines

            lines_info = list()
            for line in hunk.lines:
                line_info = dict()
                line_info['content'] = line.content
                line_info['new_lineno'] = line.new_lineno
                line_info['old_lineno'] = line.old_lineno

                if line.new_lineno > 0:
                    line_info['branches'] = find_covered_branches(coverage_data=coverage_data, file_path=new_file, line_number=line.new_lineno)

                lines_info.append(line_info)

            change_info['lines'] = lines_info
            change_info_list.append(change_info)

        report[new_file] = change_info_list

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--coverage', required=True, help='Path to the gcovr json code coverage data file')
    parser.add_argument('-g', '--git_root', required=True, help='path of the Git working directory')
    parser.add_argument('-p', '--git_patch', help='Path to the git patch file')
    parser.add_argument('-o', '--outfile', help='Path of the file to write output to')
    parser.add_argument('-v', '--verbose', action="store_true", help='be verbose')
    args = parser.parse_args()

    verbose = args.verbose

    if verbose:
        print('Code Coverage Analysis')
        print('======================')
        print('Configuration:')
        print('  Coverage data file:  {}'.format(args.coverage))
        print('  Git root path:  {}'.format(args.git_root))
        print('  Git patch path:  {}'.format(args.git_patch))
        print('  Output file:  {}'.format(args.outfile))

    git_working_tree_dir = args.git_root
    patch_info = get_git_info(git_working_tree_dir=git_working_tree_dir, verbose=verbose)
    coverage_data = read_coverage_data(args.coverage)

    report_info = create_report_info(patch_info=patch_info, coverage_data=coverage_data)

    if args.outfile is not None:
        report_as_json_object = json.dumps(report_info, indent=2)
        with open(args.outfile, "w") as output_file:
            output_file.write(report_as_json_object)


if __name__ == '__main__':
    main()
