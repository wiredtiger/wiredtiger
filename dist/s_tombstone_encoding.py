#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Check that the layered cursor keeps tombstone value encoding wired up correctly.
#
# Layered tables escape user values that collide with the reserved ingest tombstone marker (see
# the "Tombstone value encoding" comment in src/cursor/cur_layered.c). Two invariants keep the
# escaping consistent, and both are easy to break silently because the escaped values are rare:
#
#   1. Every value promoted from a constituent cursor up to the user-facing layered cursor must be
#      run through __clayered_deleted_decode() (or the __clayered_decode_current() wrapper) before
#      it is exposed. A read path that copies current_cursor->value into cursor->value and returns
#      without decoding hands the caller the escaped bytes (the WT-17905 class of bug).
#
#   2. Every __clayered_deleted_encode()/__clayered_deleted_decode() call must take its full
#      argument list, carrying a constituent-derived decision (does this value belong to, or come
#      from, the stable table?) as its third argument. A call that drops the argument or passes an
#      unrelated value escapes or strips against the wrong table.
#
#   3. Every value drained from the ingest table into the stable table (in
#      src/conn/conn_layered_ingest.c) must be converted to the stable form before it is stored, so
#      an unescaped stable on-disk image never inherits the escape byte.
#
# This is a structural lint, not a proof: it flags promotions with no nearby decode, malformed
# encode/decode calls, and drain allocations that skip the stable conversion. Sites that are
# genuinely allowed to skip a decode are listed, with a ticket, in ALLOWED_MISSING_DECODE below
# rather than being silently ignored.

import os
import re
import sys

from common_functions import filter_if_fast

# The layered cursor read/write paths. The prefix must match how filter_if_fast sees changed file
# names (see its docstring).
TARGET = "src/cursor/cur_layered.c"
PREFIX = "../"

# The ingest->stable drain lives in a second file. Values copied from the ingest table (always
# escaped) into the stable table must be converted to the stable form before they are stored, or an
# unescaped stable on-disk image inherits the escape byte. Two drain paths carry a value: the
# standard-value allocation and the prepared-transaction fix that redirects an in-flight update onto
# the stable btree.
INGEST_TARGET = "src/conn/conn_layered_ingest.c"

# A standard-value update allocated on the drain path. Real tombstones use __wt_upd_alloc_tombstone
# and carry no user value, so they are not matched.
UPD_ALLOC_STANDARD_RE = re.compile(r"__wt_upd_alloc\([^;]*WT_UPDATE_STANDARD")
STABLE_VALUE_STRIP = "__wt_clayered_ingest_to_stable_value("
STABLE_UPDATE_STRIP = "__wt_clayered_ingest_to_stable_update("
PREPARED_FIX_FUNC = "__layered_fix_prepared_transaction_callback"

# A value belonging to one of the constituent cursors. These are the sources that carry encoded
# bytes and so must be decoded before reaching the user, and the destinations that must be encoded.
CONSTITUENT = (
    r"clayered->current_cursor|current|op\.stable|op\.ingest|"
    r"clayered->stable_cursor|clayered->ingest_cursor|c_stable|c_ingest"
)

# A promotion of a constituent value into the user-facing layered cursor (cursor/iface is always
# &clayered->iface in this file).
PROMOTE_RE = re.compile(
    r"WT_ITEM_SET\((?:cursor|iface)->value\s*,\s*(?:" + CONSTITUENT + r")->value\)")

# The dominant read idiom never touches WT_ITEM_SET: a lookup, or a constituent get_value, writes
# straight into the user cursor's value and the caller decodes afterwards. The destination is pinned
# to the user-facing value so the write paths, which look a value up into a local WT_ITEM before
# re-encoding it, are not mistaken for reads that still owe a decode.
LOOKUP_PROMOTE_RE = re.compile(
    r"(?:__clayered_lookup|get_value)\([^;]*&(?:cursor|iface)->value\s*\)")

DECODE_CALL = "__clayered_deleted_decode("
ENCODE_CALL = "__clayered_deleted_encode("

# The thin wrapper that decodes the layered cursor's current constituent; it satisfies the Rule 1
# decode obligation just as a direct __clayered_deleted_decode() call does.
DECODE_WRAPPER = "__clayered_decode_current("

# The 0-based index of the constituent-decision argument in an encode/decode call: for encode
# (session, value, to_stable, final_value, tmpp) and decode (session, value, from_stable) alike it
# is the third argument.
DECISION_ARG = 2

# Tokens that make the constituent-decision argument legitimate. A bare boolean literal is allowed
# because the modify helpers know their target table statically.
ARG_OK = re.compile(r"stable_cursor|current_cursor|ingest|^\s*(?:true|false)\b")

# The reserved marker itself: the __wt_tombstone global, or a literal 0x14 / \x14 byte. Escaping and
# stripping must stay inside the sanctioned helpers so the stable-encoding switch remains the only
# place deciding whether stable values participate; a hand-rolled marker test or byte append
# anywhere else would silently bypass it.
RAW_MARKER_RE = re.compile(r"__wt_tombstone|(?:0x14|\\x14)(?![0-9a-fA-F])")

# Recording a delete writes the real marker into a constituent; that is not a hand-rolled escape.
TOMBSTONE_WRITE_RE = re.compile(r"set_value\([^;]*&__wt_tombstone")

# The only functions allowed to handle the raw marker: the shared namespace test, the encode/decode
# helpers, and the stat/drain helpers that classify the escaped stable form.
TOMBSTONE_ALLOWED = {
    "__clayered_value_in_tombstone_namespace",
    "__clayered_deleted_encode",
    "__clayered_deleted_decode",
    "__wt_clayered_stable_value_stat",
    "__wt_clayered_ingest_to_stable_value",
}

# Functions whose promotion is knowingly not decoded, mapped to the ticket documenting why. Keyed by
# the enclosing function so the check still fires on any *new* promotion added elsewhere in the same
# function; when a skip applies the ticket is printed rather than the site being silently dropped.
# Empty: every read path now decodes.
ALLOWED_MISSING_DECODE = {}


def split_functions(lines):
    # WiredTiger style puts a function body's opening and closing brace in column zero, and inner
    # blocks are indented, so a standalone "{" / "}" pair delimits one function body. Yield
    # (name, start_line_index, end_line_index) spanning the body of each function.
    funcs = []
    name = None
    start = None
    for i, line in enumerate(lines):
        if line == "{" and start is None:
            # The signature is the non-empty line above the brace; pull the identifier from it.
            for j in range(i - 1, -1, -1):
                m = re.search(r"(\w+)\s*\(", lines[j])
                if m:
                    name = m.group(1)
                    break
            start = i
        elif line == "}" and start is not None:
            funcs.append((name, start, i))
            name = start = None
    return funcs


def call_arg_count(text, open_paren):
    # Given text and the index of the "(" opening a call, return the number of top-level comma
    # separated arguments and the list of their raw texts, or (None, None) if the call is not
    # closed within the text.
    depth = 0
    args = []
    cur = []
    i = open_paren
    while i < len(text):
        c = text[i]
        if c == "(":
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif c == ")":
            depth -= 1
            if depth == 0:
                args.append("".join(cur))
                return len(args), args
        if depth == 1 and c == ",":
            args.append("".join(cur))
            cur = []
        else:
            cur.append(c)
        i += 1
    return None, None


def strip_block_comments(text):
    # Replace every /* ... */ block, including multi-line ones, with as many newlines as it spanned.
    # Line numbers are preserved so findings still point at real source lines, and a marker or a
    # "decode" mention that lives only in prose (the encoding overview, a function header) cannot
    # trip the scanners.
    def repl(m):
        return "\n" * m.group(0).count("\n")

    return re.sub(r"/\*.*?\*/", repl, text, flags=re.DOTALL)


def check_text(text, skips=None):
    problems = []
    code = strip_block_comments(text).split("\n")
    funcs = split_functions(code)

    def enclosing(idx):
        for name, s, e in funcs:
            if s <= idx <= e:
                return name, s, e
        return None, idx, idx

    # Rule 1: a constituent value surfaced on the user cursor -- copied with WT_ITEM_SET, or
    # looked up straight into cursor->value -- must be followed by a decode in the same function.
    for i, ln in enumerate(code):
        promote = PROMOTE_RE.search(ln)
        lookup = LOOKUP_PROMOTE_RE.search(ln)
        if not promote and not lookup:
            continue
        name, _, end = enclosing(i)
        if any(
          DECODE_CALL in code[j] or DECODE_WRAPPER in code[j] for j in range(i, end + 1)):
            continue
        if name in ALLOWED_MISSING_DECODE:
            if skips is not None:
                skips.append(
                    f"{TARGET}:{i + 1}: decode intentionally skipped in {name}() "
                    f"({ALLOWED_MISSING_DECODE[name]})")
            continue
        how = "promoted to" if promote else "looked up into"
        problems.append(
            f"{TARGET}:{i + 1}: value {how} the layered cursor in {name}() is not passed "
            f"through {DECODE_CALL}); the caller may see escaped tombstone bytes")

    # Rule 2: encode/decode calls must be well-formed and carry a constituent decision. Join each
    # call's (possibly wrapped) text so a call split across lines is still parsed.
    joined = "\n".join(code)
    for token, want in ((ENCODE_CALL, 5), (DECODE_CALL, 3)):
        for m in re.finditer(re.escape(token), joined):
            # Skip the function definition itself (column zero, i.e. preceded by a newline).
            if m.start() == 0 or joined[m.start() - 1] == "\n":
                continue
            open_paren = m.start() + len(token) - 1
            count, args = call_arg_count(joined, open_paren)
            lineno = joined.count("\n", 0, m.start()) + 1
            if count is None:
                continue
            if count != want:
                problems.append(
                    f"{TARGET}:{lineno}: {token}) called with {count} arguments, expected {want}")
            else:
                decision = args[DECISION_ARG]
                if not ARG_OK.search(decision.strip()):
                    problems.append(
                        f"{TARGET}:{lineno}: {token}) argument '{decision.strip()}' is not a "
                        f"constituent decision (expected a stable/ingest/current_cursor test or "
                        f"a true/false literal)")

    # Rule 3: the raw tombstone marker may only appear inside the sanctioned helpers. Anywhere else
    # it is a hand-rolled escape or strip that bypasses the stable-encoding switch; writing the real
    # marker to record a delete is exempt.
    for i, ln in enumerate(code):
        if not RAW_MARKER_RE.search(ln) or TOMBSTONE_WRITE_RE.search(ln):
            continue
        name, _, _ = enclosing(i)
        if name in TOMBSTONE_ALLOWED:
            continue
        problems.append(
            f"{TARGET}:{i + 1}: raw tombstone marker used in {name}() outside the sanctioned "
            f"encode/decode helpers; a hand-rolled escape bypasses the stable-encoding switch")

    return problems


def check_ingest_text(text):
    problems = []
    code = strip_block_comments(text).split("\n")
    funcs = split_functions(code)

    def enclosing(idx):
        for name, s, e in funcs:
            if s <= idx <= e:
                return name, s, e
        return None, idx, idx

    # Rule B1: a standard value drained into the stable table must be converted to the stable form
    # first; the conversion must appear before the allocation in the same function. Real tombstones
    # use __wt_upd_alloc_tombstone and carry no value, so they are not matched.
    for i, ln in enumerate(code):
        if not UPD_ALLOC_STANDARD_RE.search(ln):
            continue
        name, start, _ = enclosing(i)
        if any(STABLE_VALUE_STRIP in code[j] for j in range(start, i + 1)):
            continue
        problems.append(
            f"{INGEST_TARGET}:{i + 1}: a standard value is drained to the stable table in {name}() "
            f"without {STABLE_VALUE_STRIP}); an unescaped stable image inherits the escape byte")

    # Rule B2: the prepared-transaction fix redirects an in-flight update onto the stable btree and
    # must strip that update too. A presence check on the redirecting function catches its removal.
    for name, s, e in funcs:
        if name != PREPARED_FIX_FUNC:
            continue
        if not any(STABLE_UPDATE_STRIP in code[j] for j in range(s, e + 1)):
            problems.append(
                f"{INGEST_TARGET}:{s + 1}: {PREPARED_FIX_FUNC}() redirects an update to the stable "
                f"table without {STABLE_UPDATE_STRIP}); a prepared value keeps its escape byte")

    return problems


def check(path):
    with open(path) as f:
        return check_text(f.read())


def main():
    # Resolve targets from this script's location so the check behaves the same whether it is run
    # from dist/ (as s_all does) or from the repository root. Two files are inspected: the layered
    # cursor read/write paths, and the ingest->stable drain.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    problems = []
    skips = []
    for target in (TARGET, INGEST_TARGET):
        path = os.path.normpath(os.path.join(script_dir, "..", target))
        # If a target has been renamed or moved, fail loudly rather than parse a missing path or
        # quietly pass; the constant must be updated to match.
        if not os.path.exists(path):
            print(f"{target} is missing or was renamed; update {os.path.basename(__file__)}")
            return 1
        # In fast mode only inspect a target that actually changed.
        if not list(filter_if_fast(iter([PREFIX + target]), prefix=PREFIX)):
            continue
        with open(path) as f:
            text = f.read()
        problems += check_text(text, skips) if target == TARGET else check_ingest_text(text)
    for note in skips:
        print(note)
    for p in sorted(problems):
        print(p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
