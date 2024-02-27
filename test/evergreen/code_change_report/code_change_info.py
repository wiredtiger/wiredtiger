import argparse
import csv
import json
from pygit2 import discover_repository, Repository, Diff
from pygit2 import GIT_SORT_NONE
from change_info import ChangeInfo
from code_change_helpers import is_useful_line


# This function reads a gcovr json file into a dict and returns it
def read_coverage_data(coverage_data_path: str) -> dict:
    with open(coverage_data_path) as json_file:
        data = json.load(json_file)
        return data


def read_complexity_data(complexity_data_path: str) -> list:
    complexity_data = []
    with open(complexity_data_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            complexity_data.append(row)
    return complexity_data


def preprocess_complexity_data(complexity_data: list) -> dict:
    preprocessed_complexity_data = dict()

    for complexity_item in complexity_data:
        # Strip any (leading) './'
        filename = complexity_item["file"].replace("./", "src/")
        complexity_item["file"] = filename

        if filename not in preprocessed_complexity_data:
            preprocessed_complexity_data[filename] = {}

        if complexity_item["type"] == "function":
            function_name = complexity_item["region"]
            preprocessed_complexity_data[filename][function_name] = complexity_item

    return preprocessed_complexity_data


def diff_to_change_list(diff: Diff, verbose: bool) -> dict:
    change_list = dict()
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


def get_git_diff(git_working_tree_dir: str, verbose: bool):
    repository_path = discover_repository(git_working_tree_dir)
    assert repository_path is not None

    repo = Repository(repository_path)
    latest_commit = repo.head.target
    commits = list(repo.walk(latest_commit, GIT_SORT_NONE))

    if verbose:
        print("Num commits found:{}".format(len(commits)))

    commit = commits[0]

    message_lines = commit.message.splitlines()
    message_first_line = message_lines[0]

    if verbose:
        print("{}: {}".format(commit.hex, message_first_line))

    commit_url = 'https://github.com/wiredtiger/wiredtiger/commit/{}'.format(commit.id)

    if verbose:
        print("  Files changed in {} ({})".format(commit.short_id, commit_url))

    prev_commit = commit.parents[0]
    diff = prev_commit.tree.diff_to_tree(commit.tree)

    return diff


def find_file_in_coverage_data(coverage_data: dict, file_path: str):
    file_data = None

    for file in coverage_data['files']:
        if file['file'] == file_path:
            file_data = file

    return file_data


def find_line_data(coverage_data: dict, file_path: str, line_number: int):
    line_data = None

    file_data = find_file_in_coverage_data(coverage_data=coverage_data, file_path=file_path)

    if file_data is not None:
        for line_info in file_data['lines']:
            if line_info['line_number'] == line_number:
                line_data = line_info

    return line_data


def find_covered_branches(coverage_data: dict, file_path: str, line_number: int):
    branches = list()

    line_data = find_line_data(coverage_data=coverage_data, file_path=file_path, line_number=line_number)

    if line_data is not None:
        branches = line_data['branches']

    return branches


def find_line_coverage(coverage_data: dict, file_path: str, line_number: int):
    line_coverage = -1

    line_data = find_line_data(coverage_data=coverage_data, file_path=file_path, line_number=line_number)

    if line_data is not None:
        line_coverage = line_data['count']

    return line_coverage


def get_function_coverage(function_name: str, function_file: str, start_line_number: int, end_line_number: int,
                          coverage_data: dict):
    num_lines_in_function = 0
    num_covered_lines_in_function = 0
    num_branches_in_function = 0
    num_covered_branches_in_function = 0

    for file_data in coverage_data['files']:
        if file_data['file'] == function_file:
            for line_info in file_data['lines']:
                if start_line_number <= line_info['line_number'] <= end_line_number:
                    num_lines_in_function += 1
                    if int(line_info['count']) > 0:
                        num_covered_lines_in_function += 1
                    for branch_info in line_info['branches']:
                        num_branches_in_function += 1
                        if int(branch_info['count']) > 0:
                            num_covered_branches_in_function += 1

    function_coverage = {'num_lines_in_function': num_lines_in_function,
                         'num_covered_lines_in_function': num_covered_lines_in_function,
                         'num_branches_in_function': num_branches_in_function,
                         'num_covered_branches_in_function' : num_covered_branches_in_function}

    return function_coverage


def get_function_info(file_path: str,
                      line_number: int,
                      preprocessed_complexity_data: dict,
                      preprocessed_prev_complexity_data: dict,
                      coverage_data: dict):
    function_info = dict()

    if file_path in preprocessed_complexity_data:
        file_complexity_detail = preprocessed_complexity_data[file_path]

        for function_name in file_complexity_detail:
            complexity_detail = file_complexity_detail[function_name]
            detail_file = complexity_detail['file']
            detail_start_line_number = int(complexity_detail['line start'])
            detail_end_line_number = int(complexity_detail['line end'])
            if detail_file == file_path and detail_start_line_number <= line_number <= detail_end_line_number:
                function_info['name'] = complexity_detail['region']
                function_info['complexity'] = int(complexity_detail['std.code.complexity:cyclomatic'])
                function_info['lines_of_code'] = int(complexity_detail['std.code.lines:code'])

                function_coverage = get_function_coverage(function_name=function_name,
                                                          function_file=detail_file,
                                                          start_line_number=detail_start_line_number,
                                                          end_line_number=detail_end_line_number,
                                                          coverage_data=coverage_data)

                function_info.update(function_coverage)

                if preprocessed_prev_complexity_data is not None:
                    file_prev_info = preprocessed_prev_complexity_data[detail_file]
                    if function_name in file_prev_info:
                        function__prev_info = file_prev_info[function_name]
                        function_info['prev_complexity'] = int(function__prev_info['std.code.complexity:cyclomatic'])
                        function_info['prev_lines_of_code'] = int(function__prev_info['std.code.lines:code'])
                break

    return function_info


def get_num_branches_covered(branch_coverage_info: list) -> int:
    num_branches_covered = 0
    for branch in branch_coverage_info:
        if branch['count'] > 0:
            num_branches_covered += 1
    return num_branches_covered

def create_report_info(change_list: dict,
                       coverage_data: dict,
                       preprocessed_complexity_data: dict,
                       preprocessed_prev_complexity_data: dict):
    changed_function_info = dict()
    file_change_list = dict()

    change_num_lines = 0
    change_num_lines_covered = 0
    change_num_branches = 0
    change_num_branched_covered = 0

    change_branch_coverage = 0

    for new_file in change_list:
        this_patch = change_list[new_file]
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
                    line_coverage = find_line_coverage(coverage_data=coverage_data,
                                                       file_path=new_file,
                                                       line_number=line.new_lineno)
                    branch_coverage = find_covered_branches(coverage_data=coverage_data,
                                                            file_path=new_file,
                                                            line_number=line.new_lineno)
                    line_info['count'] = line_coverage
                    line_info['branches'] = branch_coverage
                    if line.old_lineno < 0:
                        # Ihe line was changed, so update the counts for the overall change
                        if line_coverage >= 0 and is_useful_line(line.content):
                            change_num_lines += 1
                            if line_coverage > 0:
                                change_num_lines_covered += 1
                        change_num_branches += len(branch_coverage)
                        change_num_branched_covered += get_num_branches_covered(branch_coverage_info=branch_coverage)

                    if preprocessed_complexity_data:
                        function_info = get_function_info(file_path=new_file,
                                                          line_number=line.new_lineno,
                                                          preprocessed_complexity_data=preprocessed_complexity_data,
                                                          preprocessed_prev_complexity_data=preprocessed_prev_complexity_data,
                                                          coverage_data=coverage_data)
                        if function_info:
                            if new_file not in changed_function_info:
                                changed_function_info[new_file] = dict()
                            function_name = function_info['name']
                            changed_function_info[new_file][function_name] = function_info

                lines_info.append(line_info)

            change_info['lines'] = lines_info
            change_info_list.append(change_info)

            file_change_list[new_file] = change_info_list

    report = dict()
    report['summary_info'] = {'num_lines': change_num_lines,
                              'num_lines_covered': change_num_lines_covered,
                              'num_branches': change_num_branches,
                              'num_branches_covered': change_num_branched_covered}
    report['change_info_list'] = file_change_list
    report['changed_functions'] = changed_function_info

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--coverage', required=True, help='Path to the gcovr json code coverage data file')
    parser.add_argument('-m', '--metrix_complexity_data', help='Path to the Metrix++ complexity data csv file')
    parser.add_argument('-p', '--prev_metrix_complexity_data',
                        help='Path to the Metrix++ complexity data csv file for the previous version')
    parser.add_argument('-g', '--git_root', required=True, help='path of the Git working directory')
    parser.add_argument('-d', '--git_diff', help='Path to the git diff file')
    parser.add_argument('-o', '--outfile', required=True, help='Path of the file to write output to')
    parser.add_argument('-v', '--verbose', action="store_true", help='be verbose')
    args = parser.parse_args()

    verbose = args.verbose
    git_diff = args.git_diff
    git_working_tree_dir = args.git_root
    complexity_data_file = args.metrix_complexity_data
    prev_complexity_data_file = args.prev_metrix_complexity_data
    preprocessed_complexity_data = None
    preprocessed_prev_complexity_data = None

    if verbose:
        print('Code Coverage Analysis')
        print('======================')
        print('Configuration:')
        print('  Coverage data file:  {}'.format(args.coverage))
        print('  Complexity data file: {}'.format(complexity_data_file))
        print('  Complexity data file: {}'.format(complexity_data_file))
        print('  Git root path:  {}'.format(git_working_tree_dir))
        print('  Git diff path:  {}'.format(git_diff))
        print('  Output file:  {}'.format(args.outfile))

    coverage_data = read_coverage_data(args.coverage)

    if complexity_data_file is not None:
        complexity_data = read_complexity_data(complexity_data_file)
        preprocessed_complexity_data = preprocess_complexity_data(complexity_data=complexity_data)

    if prev_complexity_data_file is not None:
        prev_complexity_data = read_complexity_data(prev_complexity_data_file)
        preprocessed_prev_complexity_data = preprocess_complexity_data(complexity_data=prev_complexity_data)

    if git_diff is None:
        diff = get_git_diff(git_working_tree_dir=git_working_tree_dir, verbose=verbose)
    else:
        file = open(git_diff, mode="r")
        data = file.read()
        diff = Diff.parse_diff(data)

    change_list = diff_to_change_list(diff=diff, verbose=verbose)
    report_info = create_report_info(change_list=change_list,
                                     coverage_data=coverage_data,
                                     preprocessed_complexity_data=preprocessed_complexity_data,
                                     preprocessed_prev_complexity_data=preprocessed_prev_complexity_data)

    report_as_json_object = json.dumps(report_info, indent=2)
    with open(args.outfile, "w") as output_file:
        output_file.write(report_as_json_object)


if __name__ == '__main__':
    main()
