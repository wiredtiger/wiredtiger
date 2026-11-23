/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "test_util.h"

/*
 * A targeted hot backup reports an LSM tree's metadata entry (the chunk list), then walks the
 * tree's chunk array to collect the chunk and bloom files to copy. A merge installs its new chunk
 * stack while holding the LSM tree write lock, so the entry must be reported while the tree lock is
 * held: a merge installing between the two steps otherwise leaves the backup metadata describing
 * chunks whose files were never copied, and restoring the backup panics an LSM worker with ENOENT.
 *
 * Run the same walk the backup runs, with a name callback that checks the tree lock is held while
 * the entry is reported and that the entry describes the chunk array the walk is about to read.
 *
 * The tree is left empty so that nothing else can take the tree lock during the walk: with no
 * writes there is no work for the LSM workers, so the callback's lock check is deterministic.
 */

static const char *const uri = "lsm:main";

static struct {
    u_int calls;        /* Metadata entries reported */
    u_int chunk_calls;  /* Chunk files reported */
    u_int entry_chunks; /* Chunks listed in the metadata entry */
    u_int walk_chunks;  /* Chunks in the tree's array */
    bool lock_held;     /* Tree lock held while the entry was reported */
} state;

/*
 * chunks_in_entry --
 *     Return the number of chunks listed in an LSM metadata entry.
 */
static u_int
chunks_in_entry(WT_SESSION_IMPL *session, const char *entry)
{
    WT_CONFIG chunks;
    WT_CONFIG_ITEM k, v;
    u_int n;

    if (__wt_config_getones(session, entry, "chunks", &v) != 0)
        return (UINT_MAX);
    __wt_config_subinit(session, &chunks, &v);
    for (n = 0; __wt_config_next(&chunks, &k, &v) == 0;)
        if (WT_CONFIG_LIT_MATCH("id", k))
            ++n;
    return (n);
}

/*
 * name_cb --
 *     Report a metadata entry or a chunk file, checking what a hot backup relies on.
 */
static int
name_cb(WT_SESSION_IMPL *session, const char *name, bool *skipp)
{
    WT_DECL_RET;
    WT_LSM_TREE *lsm_tree;
    char *value;

    *skipp = false;

    if (!WT_PREFIX_MATCH(name, "lsm:")) {
        ++state.chunk_calls;
        return (0);
    }

    ++state.calls;

    WT_RET(__wt_lsm_tree_get(session, name, false, &lsm_tree));

    /*
     * A merge changes the chunk array with only the tree write lock held, so the entry has to be
     * reported with the tree lock held for the walk that follows to see the same chunk array.
     */
    if (__wt_try_writelock(session, &lsm_tree->rwlock) == 0) {
        state.lock_held = false;
        __wt_writeunlock(session, &lsm_tree->rwlock);
    } else
        state.lock_held = true;

    /* The entry has to describe the chunk array the walk is about to read. */
    WT_ERR(__wt_metadata_search(session, name, &value));
    state.entry_chunks = chunks_in_entry(session, value);
    state.walk_chunks = lsm_tree->nchunks;
    __wt_free(session, value);

err:
    __wt_lsm_tree_release(session, lsm_tree);
    return (ret);
}

/*
 * main --
 *     Run the walk a targeted hot backup runs over an LSM tree.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);
    testutil_check(wiredtiger_open(opts->home, NULL, "create", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    testutil_check(session->create(session, "table:main", "type=lsm,key_format=S,value_format=S"));

    testutil_check(__wt_schema_worker((WT_SESSION_IMPL *)session, uri, NULL, name_cb, NULL, 0));

    testutil_assert(state.calls == 1);
    testutil_assert(state.chunk_calls == 0);
    testutil_assert(state.lock_held);
    testutil_assert(state.entry_chunks == state.walk_chunks);

    testutil_cleanup(opts);
    return (0);
}
