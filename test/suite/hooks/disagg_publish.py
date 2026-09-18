#!/usr/bin/env python
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
#
# [TEST_TAGS]
# ignored_file
# [END_TAGS]


def _query_schema_epoch(connection, name):
    """Return the named schema epoch as an integer."""
    return int(connection.query_timestamp(f"get={name}"), 16)


def seed_stable_schema_epoch(connection):
    """Initialize the stable schema epoch on a newly opened connection."""
    # Stable is unset after open but the last checkpoint tells us where to resume.
    last_checkpoint_epoch = _query_schema_epoch(
        connection, "last_disaggregated_schema_epoch"
    )

    # Zero disables schema epochs, therefore we should start new databases at 1.
    epoch = max(last_checkpoint_epoch, 1)
    connection.set_timestamp(f"stable_disaggregated_schema_epoch={epoch:x}")


def publish_then_advance_stable_epoch(session, uri):
    """Publish a schema change and advance stable. Callers must exclude no-ops."""
    connection = session.connection

    # Advancing stable after each publish lets us use it as the epoch counter.
    epoch = _query_schema_epoch(connection, "stable_disaggregated_schema_epoch") + 1
    session.publish(uri, f"disaggregated=(schema_epoch={epoch:x})")

    # Advance now so writes can evict pages before the next checkpoint.
    connection.set_timestamp(f"stable_disaggregated_schema_epoch={epoch:x}")
