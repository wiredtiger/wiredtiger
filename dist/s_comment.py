import sys

comment = str()
in_multiline = False
line_length = 100
indentation = 0

for line in sys.stdin:
    sline = line.strip()
    # Beginning of a multi-line comment.
    if sline.startswith('/*') and not sline.endswith('*/'):
        assert not in_multiline
        in_multiline = True
        # Figure out how far we need to indent.
        for c in line:
            if c == ' ':
                indentation += 1
            else:
                break
    # End of a comment. If we were in the middle of a multi-line comment then
    # write it.
    elif sline.endswith('*/'):
        if in_multiline:
            sys.stdout.write('{}/*\n'.format(' ' * indentation))
            words = comment.split()
            current_line = '{}'.format(' ' * indentation)
            for word in words:
                if len(current_line) + len(word) > line_length:
                    sys.stdout.write(current_line + '\n')
                    current_line = '{}'.format(' ' * indentation)
                current_line += (' ' + word)
            sys.stdout.write(current_line + '\n')
            sys.stdout.write('{}*/\n'.format(' ' * indentation))
        comment = str()
        in_multiline = False
        indentation = 0
    elif in_multiline:
        # Trim asterisks at the beginning of each line in a multi-line comment.
        if sline.startswith('*'):
            sline = sline[1:]
        # Might be trailing whitespace after the asterisk. Leading strip again.
        comment += sline.lstrip()
    else:
        sys.stdout.write(line)
