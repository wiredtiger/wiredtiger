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

# Self-test for s_tombstone_encoding.py, the layered tombstone value encoding lint. Feeds small
# in-memory snippets through check_text() and asserts the finding count, then runs the check against
# the real src/cursor/cur_layered.c as a golden regression guard against false positives. Silent and
# exit 0 on success so s_all stays clean; prints the mismatches and exits 1 on any failure.

import os
import sys

import s_tombstone_encoding as ste

FAILURES = []


def expect(name, text, count, contains=None):
    problems = ste.check_text(text)
    ok = len(problems) == count
    if contains is not None:
        ok = ok and any(contains in p for p in problems)
    if not ok:
        FAILURES.append((name, count, contains, problems))


def expect_ingest(name, text, count, contains=None):
    problems = ste.check_ingest_text(text)
    ok = len(problems) == count
    if contains is not None:
        ok = ok and any(contains in p for p in problems)
    if not ok:
        FAILURES.append((name, count, contains, problems))


# A promotion followed by a decode in the same function is correct: no finding.
expect("promotion with decode", """
static int
__clayered_reader(WT_CURSOR *cursor)
{
    WTI_CURSOR_LAYERED *clayered;

    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    __clayered_deleted_decode(
      session, &cursor->value, clayered->current_cursor == clayered->stable_cursor);
    return (0);
}
""", 0)

# A promotion with no decode hands the caller escaped bytes.
expect("promotion without decode", """
static int
__clayered_bad_reader(WT_CURSOR *cursor)
{
    WTI_CURSOR_LAYERED *clayered;

    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    return (0);
}
""", 1, contains="promoted to")

# The dominant read idiom: a lookup writing straight into cursor->value still owes a decode.
expect("lookup into cursor->value without decode", """
static int
__clayered_search(WT_CURSOR *cursor)
{
    WTI_CLAYERED_OP op;

    WT_ERR(__clayered_lookup(&op, &cursor->value));
    return (0);
}
""", 1, contains="looked up into")

# A write path looks a value up into a local WT_ITEM before re-encoding it; it must not be flagged.
expect("lookup into a local value is not a read", """
static int
__clayered_writer(WT_CURSOR *cursor)
{
    WTI_CLAYERED_OP op;
    WT_ITEM value;

    WT_ERR(__clayered_lookup(&op, &value));
    return (0);
}
""", 0)

# An encode call missing its final table decision argument.
expect("encode with wrong argument count", """
static int
__clayered_encoder(WT_CURSOR *cursor)
{
    WT_ITEM value, *buf;

    WT_ERR(__clayered_deleted_encode(session, &cursor->value, &value, &buf));
    return (0);
}
""", 1, contains="arguments, expected 5")

# A decode whose final argument is not a constituent decision.
expect("decode with a bad final argument", """
static int
__clayered_decoder(WT_CURSOR *cursor)
{
    __clayered_deleted_decode(session, &cursor->value, some_flag);
    return (0);
}
""", 1, contains="is not a")

# An encode whose constituent-decision (third) argument is not a table decision.
expect("encode with a bad decision argument", """
static int
__clayered_encoder2(WT_CURSOR *cursor)
{
    WT_ITEM value, *buf;

    WT_ERR(__clayered_deleted_encode(session, &cursor->value, whoops, &value, &buf));
    return (0);
}
""", 1, contains="is not a")

# A well-formed encode with the table decision as its third argument is correct.
expect("encode with a good decision argument", """
static int
__clayered_encoder3(WT_CURSOR *cursor)
{
    WT_ITEM value, *buf;

    WT_ERR(__clayered_deleted_encode(session, &cursor->value, op.ingest == NULL, &value, &buf));
    return (0);
}
""", 0)

# The __clayered_decode_current() wrapper satisfies the Rule 1 decode obligation.
expect("promotion with the decode wrapper", """
static int
__clayered_reader2(WT_CURSOR *cursor)
{
    WTI_CURSOR_LAYERED *clayered;

    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    __clayered_decode_current(clayered, &cursor->value);
    return (0);
}
""", 0)

# A hand-rolled use of the raw marker outside the sanctioned helpers.
expect("raw marker in an unsanctioned function", """
static int
__clayered_sneaky(WT_CURSOR *cursor)
{
    memcpy(buf, __wt_tombstone.data, __wt_tombstone.size);
    return (0);
}
""", 1, contains="raw tombstone marker")

# The same raw marker use inside a sanctioned helper is allowed.
expect("raw marker in a sanctioned helper", """
static WT_INLINE int
__clayered_deleted_encode(WT_SESSION_IMPL *session, const WT_ITEM *value, bool to_stable,
  WT_ITEM *final_value, WT_ITEM **tmpp)
{
    memcpy((uint8_t *)tmp->mem + value->size, __wt_tombstone.data, 1);
    return (0);
}
""", 0)

# Writing the real marker to record a delete is a legitimate tombstone write, not a hand-rolled one.
expect("writing the real tombstone marker is exempt", """
static int
__clayered_delete(WT_CURSOR *cursor)
{
    cursor->set_value(cursor, &__wt_tombstone);
    return (0);
}
""", 0)

# The marker named only in a block comment must not trip the scan; line numbers stay aligned.
expect("marker mentioned only in a comment", """
/*
 * The reserved value __wt_tombstone is the two bytes {\\x14\\x14}.
 */
static int
__clayered_noop(WT_CURSOR *cursor)
{
    return (0);
}
""", 0)

# ALLOWED_MISSING_DECODE prints the documenting ticket instead of silently dropping the site.
skip_text = """
static int
__clayered_bad_reader(WT_CURSOR *cursor)
{
    WTI_CURSOR_LAYERED *clayered;

    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    return (0);
}
"""
saved = ste.ALLOWED_MISSING_DECODE
ste.ALLOWED_MISSING_DECODE = {"__clayered_bad_reader": "WT-99999: intentional legacy skip"}
try:
    skips = []
    problems = ste.check_text(skip_text, skips)
    if problems or len(skips) != 1 or "WT-99999" not in skips[0]:
        FAILURES.append(("allow-listed skip prints its ticket", 0, "WT-99999", problems + skips))
finally:
    ste.ALLOWED_MISSING_DECODE = saved

# Golden regression: the real layered cursor must be clean, guarding against false positives.
real = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ste.TARGET))
if os.path.exists(real):
    real_problems = ste.check(real)
    if real_problems:
        FAILURES.append(("real cur_layered.c is clean", 0, None, real_problems))
else:
    FAILURES.append(("real cur_layered.c is present", 0, None, [f"missing {real}"]))

# Drain (conn_layered_ingest.c): a standard value converted to the stable form before the
# allocation is correct.
expect_ingest("drain converts before the standard allocation", """
static int
__layered_copy_ingest_table(WT_SESSION_IMPL *session)
{
    if (__wt_clayered_deleted(value))
        WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
    else {
        __wt_clayered_ingest_to_stable_value(session, value);
        WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
    }
    return (0);
}
""", 0)

# A standard value allocated with no conversion inherits the escape byte on the stable image.
expect_ingest("drain standard allocation without conversion", """
static int
__layered_copy_ingest_table(WT_SESSION_IMPL *session)
{
    WT_ERR(__wt_upd_alloc(session, value, WT_UPDATE_STANDARD, &upd, NULL));
    return (0);
}
""", 1, contains="without __wt_clayered_ingest_to_stable_value")

# A real tombstone allocation carries no user value and is exempt.
expect_ingest("tombstone allocation is exempt", """
static int
__layered_copy_ingest_table(WT_SESSION_IMPL *session)
{
    WT_ERR(__wt_upd_alloc_tombstone(session, &upd, NULL));
    return (0);
}
""", 0)

# The prepared-fix path that strips the redirected update is correct.
expect_ingest("prepared fix strips the redirected update", """
static bool
__layered_fix_prepared_transaction_callback(WT_SESSION_IMPL *session)
{
    op->btree = cookie->stable_btree;
    __wt_clayered_ingest_to_stable_update(session, op->u.op_upd);
    return (true);
}
""", 0)

# The prepared-fix path missing the strip keeps the escape byte on a prepared value.
expect_ingest("prepared fix without the update strip", """
static bool
__layered_fix_prepared_transaction_callback(WT_SESSION_IMPL *session)
{
    op->btree = cookie->stable_btree;
    return (true);
}
""", 1, contains="without __wt_clayered_ingest_to_stable_update")

# Golden regression: the real ingest drain file must be clean, guarding against false positives.
real_ingest = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ste.INGEST_TARGET))
if os.path.exists(real_ingest):
    with open(real_ingest) as f:
        ingest_problems = ste.check_ingest_text(f.read())
    if ingest_problems:
        FAILURES.append(("real conn_layered_ingest.c is clean", 0, None, ingest_problems))
else:
    FAILURES.append(
        ("real conn_layered_ingest.c is present", 0, None, [f"missing {real_ingest}"]))

if FAILURES:
    for name, count, contains, problems in FAILURES:
        want = f"{count} finding(s)"
        if contains is not None:
            want += f" containing '{contains}'"
        print(f"test_tombstone_encoding: FAILED '{name}': expected {want}, got {problems}")
    sys.exit(1)
