import re, sys

words = []
in_multiline = False
line_length = 100
indentation = 0
function_desc = False
is_block = False
comment = str()

for line in sys.stdin:
    sline = line.strip()
    # Beginning of a multi-line comment.
    if (sline.startswith('/*') and '*/' not in sline and not
        sline.endswith('\\') and not sline.endswith('!')):
        comment = line
        assert not in_multiline
        in_multiline = True
        is_block = True
        # Figure out how far we need to indent.
        indentation = 0
        for c in line:
            if c == ' ':
                indentation += 1
            elif c == '\t':
                indentation += 8
            else:
                break
    # End of a comment. If we were in the middle of a multi-line comment then
    # write it.
    elif sline.endswith('*/'):
        comment += line
        if in_multiline and not is_block:
            sys.stdout.write(comment)
        elif in_multiline:
            indent_ws = ' ' * indentation
            sys.stdout.write('{}/*\n'.format(indent_ws))
            current_line = indent_ws + ' *'
            for word in words:
                if word.endswith('--'):
                    function_desc = True
                    sys.stdout.write(current_line + ' ' + word + '\n')
                    current_line = indent_ws + ' *' + ' ' * 4
                    continue
                if word == '\n':
                    sys.stdout.write(current_line + '\n')
                    sys.stdout.write(indent_ws + ' *' + '\n')
                    current_line = indent_ws + ' *'
                    continue
                if len(current_line) + len(word) >= line_length:
                    sys.stdout.write(current_line + '\n')
                    current_line = indent_ws + ' *'
                    if function_desc:
                        current_line += ' ' * 4
                current_line += ' ' + word
            sys.stdout.write(current_line + '\n')
            sys.stdout.write('{} */\n'.format(indent_ws))
        else:
            sys.stdout.write(line)
        is_block = False
        words = []
        in_multiline = False
        function_desc = False
    elif in_multiline:
        comment += line
        # We're only reformatting block comments where each line begins with a
        # space and an alphabetic character after the asterisk. The only
        # exceptions are function descriptions.
        is_block = is_block and (len(sline) > 3 and sline.startswith('*') and
                    sline[1] == ' ' and sline[2].isalpha()) or function_desc
        # Trim asterisks at the beginning of each line in a multi-line comment.
        if sline.startswith('*'):
            sline = sline[1:]
        # Might be trailing whitespace after the asterisk. Leading strip again.
        sline = sline.lstrip()
        words.extend(sline.split())
    else:
        sys.stdout.write(line)
