import re, sys

words = []
in_multiline = False
line_length = 100
indentation = 0
function_desc = False

for line in sys.stdin:
    sline = line.strip()
    # Beginning of a multi-line comment.
    if (sline.startswith('/*') and not sline.endswith('*/') and not
        sline.startswith('/*-')):
        assert not in_multiline
        in_multiline = True
        # Figure out how far we need to indent.
        indentation = 0
        for c in line:
            if c == ' ':
                indentation += 1
            else:
                break
    # End of a comment. If we were in the middle of a multi-line comment then
    # write it.
    elif sline.endswith('*/'):
        if in_multiline:
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
                if len(current_line) + len(word) > line_length:
                    sys.stdout.write(current_line + '\n')
                    current_line = indent_ws + ' *'
                    if function_desc:
                        current_line += ' ' * 4
                current_line += ' ' + word
            sys.stdout.write(current_line + '\n')
            sys.stdout.write('{} */\n'.format(indent_ws))
        else:
            sys.stdout.write(line)
        words = []
        in_multiline = False
        function_desc = False
    elif in_multiline:
        # Trim asterisks at the beginning of each line in a multi-line comment.
        if sline.startswith('*'):
            sline = sline[1:]
        # Might be trailing whitespace after the asterisk. Leading strip again.
        sline = sline.lstrip()
        # If it's just a blank line within a multi-line comment, let's preserve
        # that. It's usually to signal a different paragraph/idea.
        if not sline:
            words.append('\n')
        else:
            words.extend(sline.split())
    else:
        sys.stdout.write(line)
