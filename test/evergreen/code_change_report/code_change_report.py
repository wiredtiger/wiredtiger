import argparse
import json
import html


def read_code_change_info(code_change_info_path: str):
    with open(code_change_info_path) as json_file:
        info = json.load(json_file)
        return info


def get_branch_info(branches: list):
    branch_info = list()
    for branch in branches:
        count = branch['count']
        if count < 0:
            count = 0
        branch_info.append(count)
    return branch_info


def get_non_zero_count(value_list: list):
    non_zero_count = 0
    for value in value_list:
        if value != 0:
            non_zero_count += 1
    return non_zero_count


def get_html_colour(count: int, of: int):
    colour = ""
    if count == 0:
        colour = "LightPink"
    elif count == of:
        colour = "PaleGreen"
    else:
        colour = "SandyBrown"

    return colour


def get_complexity_html_colour(complexity: int):
    colour = ""
    if complexity <= 10:
        colour = "PaleGreen"
    elif complexity <= 20:
        colour = "Orange"
    elif complexity <= 50:
        colour = "Tomato"
    else:
        colour = "Orchid"

    return colour


def centred_text(text):
    return "<p style=\"text-align: center\">{}</p>\n".format(text)


def right_text(text):
    return "<p style=\"text-align: right\">{}</p>\n".format(text)


def line_number_to_text(code_colour, line_number):
    if line_number > 0:
        return "    <p style=\"background-color:{};text-align: right\">{}</p>\n".format(code_colour, line_number)
    else:
        return ""


def value_as_centred_text(code_colour, value):
    return "    <p style=\"background-color:{};text-align: center\">{}</p>\n".format(code_colour, value)


def generate_file_info_as_html_text(file: str, file_info: dict, verbose: bool):
    report = list()
    code_unhighlighted = "White"

    if file.startswith("src/"):
        escaped_file = html.escape(file, quote=True)
        report.append("<a id=\"{}\"></a>\n".format(escaped_file))
        report.append("<h3>File: {}</h3>\n".format(escaped_file))

        report.append("<table cellpadding=0 cellspacing=0>\n")
        report.append("  <tr>\n")
        report.append("    <th>&nbspCount&nbsp</th>\n")
        report.append("    <th>&nbspBranches&nbsp</th>\n")
        report.append("    <th>&nbspOld line&nbsp</th>\n")
        report.append("    <th>&nbspNew line&nbsp</th>\n")
        report.append("    <th>&nbsp=+-&nbsp</th>\n")
        report.append("    <th></th>\n")
        report.append("  </tr>\n")

        first_line_for_file = True

        for hunk in file_info:
            if not first_line_for_file:
                seperator = "--------"
                report.append("  <tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td></td><td></td></tr>\n".
                              format(centred_text(seperator), centred_text(seperator),
                                     right_text(seperator), right_text(seperator)))
            first_line_for_file = False

            lines = hunk['lines']
            for line in lines:
                new_lineno = line['new_lineno']
                old_lineno = line['old_lineno']
                content = line['content']
                code_colour = code_unhighlighted
                plus_minus = "="
                strikethrough = False
                if new_lineno > 0 > old_lineno:
                    plus_minus = "+"
                if old_lineno > 0 > new_lineno:
                    plus_minus = "-"
                    strikethrough = True
                count_str = ""
                if 'count' in line:
                    count = line['count']
                    useful_line = content != '\n' and content.strip() != '{' and content.strip() != '}'
                    if count >= 0 and useful_line:
                        count_str = str(count)
                        code_colour = get_html_colour(count, count)
                report.append("  <tr>\n")
                report.append("    <td>{}</td>\n".format(right_text(count_str)))
                if 'branches' in line:
                    branches = line['branches']
                    branch_info = get_branch_info(branches=branches)
                    num_branches = len(branches)
                    if num_branches > 0:
                        non_zero_count = get_non_zero_count(branch_info)
                        code_colour = get_html_colour(non_zero_count, num_branches)
                        report.append("    <td>\n")
                        report.append("      <details>\n")
                        report.append("      <summary>\n")
                        report.append("      {} of {}\n".format(non_zero_count, num_branches))
                        #  Use the same value for the low and high values so that anything less than 100% branch
                        #  coverage results in a red scale.
                        meter_high = num_branches * 0.999
                        meter_low = meter_high
                        report.append(
                            "      <meter value=\"{}\" optimum=\"{}\" max=\"{}\" high=\"{}\" low=\"{}\"></meter>".
                            format(non_zero_count, num_branches, num_branches, meter_high, meter_low))
                        report.append("      </summary>\n")
                        report.append("        <div class=\"branchDetails\">\n")
                        for branch_index in range(len(branch_info)):
                            branch_count = branch_info[branch_index]
                            report.append("        <div>")
                            if branch_count > 0:
                                report.append("&check; ")
                            else:
                                report.append("&cross; ")
                            report.append("Branch {} taken {} time(s)".format(branch_index, branch_count))
                            report.append("</div>\n")
                        report.append("      </div>\n")
                        report.append("      </details>\n")
                        report.append("    </td>\n")
                    else:
                        report.append("    <td></td>\n")
                else:
                    report.append("    <td></td>\n")

                report.append("    <td>{}</td>\n".format(line_number_to_text(code_unhighlighted, old_lineno)))
                report.append("    <td>{}</td>\n".format(line_number_to_text(code_colour, new_lineno)))
                report.append("    <td>{}</td>\n".format(centred_text(plus_minus)))
                report.append("    <td>\n")
                if strikethrough:
                    report.append("    <del>\n")
                report.append("      <p style=\"background-color:{};font-family:\'Courier New\',sans-serif;"
                              "white-space:pre\">{}</p>\n".format(
                    code_colour, html.escape(line['content'], quote=True)))
                if strikethrough:
                    report.append("    </del>\n")
                report.append("    <td>\n")

        report.append("</table>\n")

    return report


def change_string(old_value: int, new_value: int) -> str:
    result = ""
    if new_value > old_value:
        result = "&#8679;"  # up arrow
    elif new_value < old_value:
        result = "&#8681;"  # down arrow
    return result


def describe_complexity_categories():
    code_colour_ = get_complexity_html_colour(1)
    description = list()
    description.append("<table class=\"center\">\n")
    description.append("  <tr>\n")
    description.append("    <th><a href='https://en.wikipedia.org/wiki/Cyclomatic_complexity'>Cyclomatic complexity</a></th></th>\n")
    description.append("    <th><a href='https://en.wikipedia.org/wiki/Cyclomatic_complexity#Interpretation'>Risk evaluation</a></th>\n")
    description.append("  </tr>\n")
    description.append("  <tr><td> {} </td><td> Simple procedure, little risk </td></tr>\n".
                       format(value_as_centred_text(get_complexity_html_colour(1), "1-10")))
    description.append("  <tr><td> {} </td><td> More complex, moderate risk   </td></tr>\n".
                       format(value_as_centred_text(get_complexity_html_colour(11), "11-20")))
    description.append("  <tr><td> {} </td><td> Complex, high risk            </td></tr>\n".
                       format(value_as_centred_text(get_complexity_html_colour(21), "21-50")))
    description.append("  <tr><td> {} </td><td> Untestable code, very high risk    </td></tr>\n".
                       format(value_as_centred_text(get_complexity_html_colour(51), ">50")))
    description.append("</table>\n")
    return description


def generate_html_report_as_text(code_change_info: dict, verbose: bool):
    report = list()
    change_info_list = code_change_info['change_info_list']
    changed_functions = code_change_info['changed_functions']

    report.append("")

    # Create the header and style sections
    report.append("<!DOCTYPE>\n")
    report.append("<html lang=\"\">\n")
    report.append("<head>\n")
    report.append("<title>Code Change Report</title>\n")
    report.append("<style>\n")
    report.append("  .branchDetails\n")
    report.append("  {\n")
    report.append("    font-family: sans-serif;\n")
    report.append("    font-size: small;\n")
    report.append("    text-align: left;\n")
    report.append("    position: absolute;\n")
    report.append("    width: 20em;\n")
    report.append("    padding: 1em;\n")
    report.append("    background: white;\n")
    report.append("    border: solid gray 1px;;\n")
    report.append("    box-shadow: 5px 5px 10px gray;\n")
    report.append("    z-index: 1;\n")  # pop up in front of the main text
    report.append("  }\n")
    report.append("  table\n")
    report.append("  {\n")
    report.append("      border: 1px solid black\n")
    report.append("  }\n")
    report.append("  td\n")
    report.append("  {\n")
    report.append("      padding-left: 5px; padding-right: 5px;\n")
    report.append("  }\n")
    report.append("  table.center\n")
    report.append("  {\n")
    report.append("      margin-left: auto;\n")
    report.append("      margin-right: auto;\n")
    report.append("  }\n")
    report.append("</style>\n")
    report.append("</head>\n")

    report.append("<body>\n")
    report.append("<h1 style=\"text-align: center\">Code Change Report</h1>\n")

    # Create table with a list of changed files
    report.append("<table class=\"center\">\n")
    report.append("  <tr>\n")
    report.append("    <th>Changed File(s)</th>\n")
    report.append("  </tr>\n")
    for file in change_info_list:
        escaped_file = html.escape(file, quote=True)
        report.append("  <tr><td>\n")
        if file.startswith("src/"):
            report.append("    <a href = \"#{}\">\n".format(escaped_file))
        report.append("      {}\n".format(escaped_file))
        if file.startswith("src/"):
            report.append("    </a>\n")
        report.append("  </td></tr>\n")
    report.append("</table>\n")

    report.append("<p><p>")

    report.append("<h2 style=\"text-align: center\">Code Change Details</h2>\n")
    report.append(centred_text("Only files in the 'src' directory are shown below<p>\n"))

    # Create table with a list of changed functions
    report.append("<table class=\"center\">\n")
    report.append("  <tr>\n")
    report.append("    <th>File</th>\n")
    report.append("    <th>Changed Function(s)</th>\n")
    report.append("    <th>Complexity</th>\n")
    report.append("    <th> </th>")  # a column for the complexity change arrow
    report.append("    <th>Previous<br>Complexity</th>\n")
    report.append("    <th>Lines in<br>function</th>\n")
    report.append("    <th> </th>")  # a column for the lines of code change arrow
    report.append("    <th>Previous lines<br>in function</th>\n")
    report.append("  </tr>\n")
    for file in changed_functions:
        escaped_file = html.escape(file, quote=True)
        functions_info = changed_functions[file]
        for function in functions_info:
            function_info = functions_info[function]
            complexity = int(function_info['complexity'])
            prev_complexity = -1
            code_colour = get_complexity_html_colour(complexity)
            complexity_string = value_as_centred_text(code_colour, complexity)
            prev_complexity_string = centred_text("(new)")
            complexity_change_string = ""
            if 'prev_complexity' in function_info:
                prev_complexity = int(function_info['prev_complexity'])
                code_colour = get_complexity_html_colour(prev_complexity)
                prev_complexity_string = value_as_centred_text(code_colour, prev_complexity)
                complexity_change_string = change_string(old_value=prev_complexity, new_value=complexity)

            lines_in_function = int(function_info['lines_of_code'])
            lines_change_string = ""
            prev_lines_in_function_string = "(new)"
            if 'prev_lines_of_code' in function_info:
                prev_lines_in_function = int(function_info['prev_lines_of_code'])
                lines_change_string = change_string(old_value=prev_lines_in_function, new_value=lines_in_function)
                prev_lines_in_function_string = prev_lines_in_function

            report.append("  <tr>\n")
            report.append("    <td>{}</td>\n".format(escaped_file))
            report.append("    <td>{}</td>\n".format(function_info['name']))
            report.append("    <td>{}</td>\n".format(complexity_string))
            report.append("    <td>{}</td>\n".format(complexity_change_string))
            report.append("    <td>{}</td>\n".format(prev_complexity_string))
            report.append("    <td>{}</td>\n".format(centred_text(lines_in_function)))
            report.append("    <td>{}</td>\n".format(centred_text(lines_change_string)))
            report.append("    <td>{}</td>\n".format(centred_text(prev_lines_in_function_string)))
            report.append("  </tr>\n")
    report.append("</table>\n")

    report.append("<p>")

    report.extend(describe_complexity_categories())

    # Create per-file info
    for file in change_info_list:
        file_info = change_info_list[file]
        html_lines = generate_file_info_as_html_text(file=file, file_info=file_info, verbose=verbose)
        report.extend(html_lines)

    report.append("</body>\n")
    report.append("</html>\n")

    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--code_change_info', required=True, help='Path to the code change info file')
    parser.add_argument('-o', '--html_output', required=True, help='Path of the html file to write output to')
    parser.add_argument('-v', '--verbose', action="store_true", help='be verbose')
    args = parser.parse_args()

    verbose = args.verbose

    if verbose:
        print('Code Coverage Report')
        print('====================')
        print('Configuration:')
        print('  Code change info file:  {}'.format(args.code_change_info))
        print('  Html output file:  {}'.format(args.html_output))

    code_change_info = read_code_change_info(code_change_info_path=args.code_change_info)
    html_report_as_text = generate_html_report_as_text(code_change_info=code_change_info, verbose=verbose)

    with open(args.html_output, "w") as output_file:
        output_file.writelines(html_report_as_text)


if __name__ == '__main__':
    main()
