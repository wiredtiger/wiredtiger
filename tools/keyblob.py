#!/usr/bin/env python3

# Print the hex sequence representing a componsite key, suitable for
# use as the argument to -K in the wired tiger dump utility.

import sys, struct

# Wired Tiger defined key format integer types.
INT_FMTS = ['b', 'B', 'h', 'H', 'i', 'I', 'l', 'L', 'q', 'Q', 'r', 't']

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: keyblob.py format components...')
        sys.exit(1)


    # Create a format string and transform components to types
    # suitable to use as arguments in struct.pack().
    fmt = ''
    components = []
    i = 2
    try:
        for f in sys.argv[1]:
            if f in INT_FMTS:
                components.append(int(sys.argv[i]))
                fmt += f
            elif f in ['s', 'S']:
                components.append(bytes(sys.argv[i].encode()))
                if f == 'S':
                    components[-1] += b'\x00'
                # By default python s S format specifiers default to a
                # string of length 1, so modify the prepend the length
                # in bytes.
                fmt += f'{len(components[-1])}s'
            else:
                # Format 'u' here probably doesn't work.
                print(f'Unable to encode "{f}".')
                sys.exit(1)
            i += 1
    except ValueError as err:
        print(err)
        sys.exit(1)

    try:
        s = struct.pack(fmt, *components)
        print(s.hex())
    except struct.error as err:
        print(err)
        sys.exit(1)
