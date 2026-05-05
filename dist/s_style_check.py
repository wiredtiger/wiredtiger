#!/usr/bin/env python3
"""
Per-file style checks for WiredTiger source files.

Replaces the grep-based inner loop in dist/s_style.
Called from s_style via do_in_parallel with file paths relative to the
WiredTiger tree root (set as CWD by s_style via cd_top).
"""

import fnmatch
import os
import re
import string
import sys

# Characters kept by: tr -cd '[:alnum:][:space:][:punct:]'  (LC_ALL=C)
_KEEP = frozenset(
    string.ascii_letters + string.digits + string.whitespace + string.punctuation
)

# Replacement pairs for the tr/sed pass.  The "old" strings are split across
# adjacent string literals so this file does not contain the literal patterns
# and cannot corrupt itself if accidentally passed through its own checks.
_REPLACEMENTS = [
    ('(EOP' 'NOTSUPP)', '(ENOTSUP)'),
    ('(unsign' 'ed)', '(u_int)'),
    ('hazard ref' 'erence', 'hazard pointer'),
]


def _transform(text):
    """Apply tr/sed transformations; return (new_text, changed)."""
    cleaned = ''.join(c for c in text if c in _KEEP)

    out = []
    for line in cleaned.split('\n'):
        if 'for ' not in line and line.endswith(';;'):
            line = line[:-1]
        for old, new in _REPLACEMENTS:
            line = line.replace(old, new)
        out.append(line)

    new_text = '\n'.join(out)
    return new_text, new_text != text


def _grep(pattern, lines, flags=0):
    """Return list of (1-based lineno, line) where pattern matches."""
    rx = re.compile(pattern, flags)
    return [(i, l) for i, l in enumerate(lines, 1) if rx.search(l)]


def check(f):
    """Run all style checks on file f (path relative to repo root)."""
    if not os.path.exists(f):
        print(f"s_style error {f} does not exist")
        return

    fname = os.path.basename(f)
    ext = fname.rsplit('.', 1)[1] if '.' in fname else ''

    try:
        raw = open(f, 'rb').read()
        text = raw.decode('latin-1')
    except OSError as e:
        print(f"s_style: {f}: {e}")
        return

    # File modification pass
    new_text, changed = _transform(text)
    if changed:
        print(f"modifying {f}")
        with open(f, 'wb') as fh:
            fh.write(new_text.encode('latin-1'))
        text = new_text

    lines = text.split('\n')

    # C++ extension check
    if ext == 'cxx':
        print(f"{f}: C++ files must use .cpp as an extension.")

    # src/ internal library checks
    is_src_lib = (
        f.startswith('src/') and
        not f.startswith('src/os_win/') and
        not f.startswith('src/docs/') and
        not re.match(r'src/tags', f) and
        'hash_city' not in f and
        not f.startswith('src/checksum')
    )
    if is_src_lib:
        # Camel case: grep -En '\b[a-z]+[A-Z]' | grep -Ev ':[   ]+\*|"|UNCHECKED_STRING'
        # Original used grep -n so output format is "lineno:content"; the -v
        # filter pattern ':[   ]+\*' matches comment continuation lines in that format.
        hits = _grep(r'\b[a-z]+[A-Z]', lines)
        hits = [(i, l) for i, l in hits
                if not re.search(r':[\ \t]+\*|"|UNCHECKED_STRING', f'{i}:{l}')]
        if hits:
            for i, l in hits:
                print(f'{i}:{l}')
            print(f"{f}: Styling requires variables that use underscores to separate"
                  " parts of a name instead of camel casing.")

        # Return values without parentheses
        hits = _grep(r'^[^*/\"]*\s*return [^(]', lines)
        if hits:
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            print(f"Add parentheses to return values indicated below in file {f}")
            print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
            for i, l in hits:
                print(f'{i}:{l}')

    # Paired typos in comments
    typo_rx = r'\s\b([a-zA-Z]+)\s\b\1[\s.]'
    if ext == 'py':
        hits = _grep(r'#.*' + typo_rx, lines)
        hits = [(i, l) for i, l in hits if 'long long' not in l]
    elif ext in ('c', 'cpp', 'h'):
        hits = _grep(r'/?\*.*' + typo_rx, lines)
        hits = [(i, l) for i, l in hits if '@' not in l and 'long long' not in l]
    else:
        hits = _grep(typo_rx, lines)
        hits = [(i, l) for i, l in hits if '@' not in l and not l.startswith('(')]
    if hits:
        print("paired typo")
        print("============================")
        print(f)
        for _, l in hits:
            print(l)

    # .dox files: no further checks
    if ext == 'dox':
        return

    # while(0) trailing semi-colon
    hits = _grep(r'^[^}]*while \(0\);', lines)
    if hits:
        print(f"{f}: while (0) has trailing semi-colon")
        for _, l in hits:
            print(l)

    # WT_DEADLOCK deprecated
    hits = _grep(r'WT' '_DEADLOCK', lines)
    hits = [(i, l) for i, l in hits if not re.search(r'#define.WT' '_DEADLOCK', l)]
    if hits:
        print(f"{f}: WT_DEADLOCK deprecated in favor of WT_ROLLBACK")
        for _, l in hits:
            print(l)

    # sizeof(WT_UPDATE)
    if f != 'src/include/verify_build.h':
        hits = _grep(r'sizeof\(WT' r'_UPDATE\)', lines)
        if hits:
            print(f"{f}: Use WT_UPDATE_SIZE rather than sizeof(WT_UPDATE)")
            for _, l in hits:
                print(l)

    # WT_TXN_ISO_
    # Original: grep output went to stdout first, then echo message.
    if (not f.startswith('examples/c/') and
            not f.startswith('ext/') and
            f not in ('src/include/wiredtiger_ext.h', 'src/txn/txn_ext.c')):
        hits = _grep('WT' '_TXN_ISO_', lines)
        if hits:
            for _, l in hits:
                print(l)
            print(f"{f}: WT_TXN_ISO_XXX constants only for the extension API")

    # TAILQ
    # Original: grep output went to stdout first, then echo message.
    if f != 'src/include/queue.h':
        hits = _grep(r'STAILQ_|SLIST_|\bLIST_', lines)
        if hits:
            for _, l in hits:
                print(l)
            print(f"{f}: use TAILQ for all lists")

    # __wt_errno
    if (f not in ('src/include/extern.h', 'src/include/extern_posix.h',
                  'src/include/extern_win.h', 'src/include/os.h') and
            not f.startswith('src/os_common/') and
            not f.startswith('src/os_posix/') and
            not f.startswith('src/os_win/')):
        hits = _grep('__wt' '_errno', lines)
        if hits:
            print(f"{f}: upper-level code should not call __wt_errno")
            for _, l in hits:
                print(l)

    # %zu needs to be fixed for Windows
    if not f.startswith('examples/c/') and f != 'src/include/os.h':
        hits = _grep(r'%[0-9]*zu', lines)
        hits = [(i, l) for i, l in hits if 'SIZET_FMT' not in l]
        if hits:
            print(f"{f}: %zu needs to be fixed for Windows")
            for _, l in hits:
                print(l)

    # off_t
    hits = _grep(r'\boff' r'_t\b', lines)
    if hits:
        print(f"{f}: off_t type declaration, use wt_off_t")
        for _, l in hits:
            print(l)

    # qsort
    if f != 'src/include/misc.h':
        hits = _grep(r'\sqsort\(', lines)
        if hits:
            print(f"{f}: qsort call, use WiredTiger __wt_qsort instead")
            for _, l in hits:
                print(l)

    # setvbuf
    if not fnmatch.fnmatch(f, 'src/*/os_setvbuf.c'):
        hits = _grep(r'\bsetv' r'buf\b', lines)
        if hits:
            print(f"{f}: setvbuf call, use WiredTiger library replacements")
            for _, l in hits:
                print(l)

    # snprintf / vsnprintf
    if (not f.startswith('examples/c/') and
            not f.startswith('bench/') and
            not f.endswith('.cxx') and
            not f.endswith('.cpp') and
            not f.startswith('ext/') and
            f != 'src/os_posix/os_snprintf.c'):
        hits = _grep(r'[^a-z_]snprintf\(|[^a-z_]vsnprintf\(', lines)
        if hits:
            print(f"{f}: snprintf call, use WiredTiger library replacements")
            for _, l in hits:
                print(l)

    # WT_PACKED_STRUCT must come in matched pairs
    packed = _grep('WT' '_PACKED_STRUCT', lines)
    if len(packed) % 2 != 0:
        print(f"{f}: mismatched WT_PACKED_STRUCT_BEGIN/END lines")
        for _, l in packed:
            print(l)

    # Library-only: illegal function calls
    is_lib = (not f.startswith('bench/') and
              not f.startswith('dist/') and
              not f.startswith('examples/') and
              not f.startswith('ext/') and
              not f.startswith('test/'))
    if is_lib:
        if not f.endswith('/os_alloc.c') and not f.endswith('/util_misc.c'):
            hits = _grep(
                r'\s(free|strdup|strndup|malloc|calloc|realloc|sprintf)\(', lines)
            if hits:
                print(f"{f}: call to illegal function")
                for _, l in hits:
                    print(l)

        if not f.endswith('/os_strtouq.c'):
            hits = _grep(r'\sstrtouq\(', lines)
            if hits:
                print(f"{f}: call to illegal function")
                for _, l in hits:
                    print(l)

        hits = _grep(r'\sexit\(', lines)
        if hits:
            print(f"{f}: call to illegal function")
            for _, l in hits:
                print(l)

    # Explicit "ret" declaration
    if (not f.startswith('bench/') and
            not f.startswith('examples/') and
            not f.startswith('test/') and
            not f.startswith('ext/')):
        hits = _grep(r'\bret\b', lines)
        hits = [(i, l) for i, l in hits if re.search(r'int.*[, ]ret[,;]', l)]
        hits = [(i, l) for i, l in hits if not re.search(r'[()]', l)]
        if hits:
            print(f'{f}: explicit declaration of "ret"')
            for _, l in hits:
                print(l)

    # Direct use of ctype.h functions
    if (not f.startswith('bench/') and
            not f.startswith('test/csuite/') and
            not f.startswith('examples/') and
            not f.startswith('ext/') and
            not f.endswith('.py') and
            not f.endswith('.cpp') and
            not fnmatch.fnmatch(fname, 'ctype*')):
        ctype_re = (
            r'#include.*[\"</]ctype\.h[\">]'
            r'|\b(is(alnum|alpha|cntrl|digit|graph|lower|print|punct|space|upper|xdigit)'
            r'|to(lower|toupper))\('
        )
        hits = _grep(ctype_re, lines)
        if hits:
            print(f"{f}: direct use of ctype.h functions, instead of ctype.i equivalents")
            for _, l in hits:
                print(l)


def main():
    for f in sys.argv[1:]:
        check(f)


if __name__ == '__main__':
    main()
