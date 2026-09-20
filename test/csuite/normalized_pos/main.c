/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "test_util.h"
#include "wt_internal.h"

extern int __wt_optind;
extern char *__wt_optarg;

static const char *uri = "table:normalized_pos";
static const char *file_uri = "file:normalized_pos.wt";
static const int NUM_KEYS = 100000;
static const int NUM_SAMPLES = 1000;
static int verbose = 0;

/* Exact comparison of two positions without tripping the float-equality warning. */
#define DOUBLE_EQ(a, b) (!((a) < (b)) && !((a) > (b)))

/*
 * usage --
 *     Print a usage message.
 */
__attribute__((noreturn)) static void
usage(void)
{
    fprintf(stderr, "usage: %s [-h dir]\n", progname);
    exit(EXIT_FAILURE);
}

/*
 * create_btree --
 *     Setup a btree with one key per page. Soft positions work on the in-memory btree, so use an
 *     in-memory version of WiredTiger to keep things simple when reasoning about the shape of the
 *     Btree.
 */
static void
create_btree(WT_CONNECTION *conn)
{
    WT_CURSOR *cursor;
    WT_SESSION *session;
    /* 1KB string to match the 1KB pages. */
    char val_str[1000];
    /* With 100,000 keys and 1 key per page this should mean that each key maps to an
     * equivalent npos. e.g. key 50,000 should map to roughly npos 0.5, and key 12,300 to 0.123.
     * This isn't true possibly because some pages have 10 slots and others 91? */

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(session, uri,
      "key_format=Q,value_format=S,memory_page_max=1KB,leaf_page_max=1KB,allocation_size=1KB"));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    memset(val_str, 'A', 1000);
    val_str[1000 - 1] = '\0';

    for (int i = 0; i < NUM_KEYS; i++) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, val_str);
        testutil_check(cursor->insert(cursor));
    }

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
}

/*
 * test_normalized_pos --
 *     Given a key in a tree compute the normalized position (npos) of its page. Then make sure the
 *     soft position restores the same page.
 *
 * NOTE!! This is a white box test. It uses functions and types not available in the WiredTiger API.
 */
static void
test_normalized_pos(WT_CONNECTION *conn, bool in_mem,
  int (*page_from_npos_fn)(
    WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t read_flags, uint32_t walk_flags))
{
    WT_CURSOR *cursor;
    WT_DATA_HANDLE *dhandle;
    WT_REF *page_ref, *page_ref2;
    WT_SESSION *session;
    WT_SESSION_IMPL *wt_session;
    double npos, prev_npos;
    size_t path_str_offset;
    int count, count1, count2;
    char path_str[2][1024];

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    wt_session = (WT_SESSION_IMPL *)session;
    dhandle = ((WT_CURSOR_BTREE *)cursor)->dhandle;

    /*
     * Traverse the whole dataset to stabilize the tree and make sure that we don't cause page
     * splits by looking into pages.
     */
    for (int key = 0; key < NUM_KEYS; key++) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));
    }

    /*
     * Traverse the whole dataset without looking into the page content
     */
    if (verbose)
        printf("  forward\n");
    prev_npos = npos = 0.;
    page_ref2 = NULL;
    count1 = 0;
    do {
        ++count1;
        WT_WITH_DHANDLE(wt_session, dhandle, page_from_npos_fn(wt_session, &page_ref, npos, 0, 0));
        if (verbose > 1)
            printf("npos = %f, page_ref = %p\n", npos, (void *)page_ref);
        if (page_ref == NULL)
            break;

        if (in_mem)
            testutil_assertfmt(page_ref != page_ref2,
              "Got the same page twice: %p, npos = %lf, prev_npos = %lf", (void *)page_ref, npos,
              prev_npos);

        prev_npos = npos;
        page_ref2 = page_ref;
        npos = __wt_page_npos(wt_session, page_ref, 1. + 1e-5, NULL, NULL, 0);
        testutil_assertfmt(
          npos > prev_npos, "next npos(%lf) must be greater than prev_npos(%lf)", npos, prev_npos);
        WT_WITH_DHANDLE(
          wt_session, dhandle, testutil_check(__wt_page_release(wt_session, page_ref, 0)));
    } while (npos < 1.0);
    if (verbose)
        printf("  ... %d\n", count1);
    if (in_mem)
        testutil_assertfmt(count1 == NUM_KEYS,
          "should have traversed %d pages, but only traversed %d", NUM_KEYS, count1);
    /* For on-disk database, there's no guarantee that it's one key per page */

    /*
     * And the other way around.
     */
    if (verbose)
        printf("  backwards\n");
    prev_npos = npos = 1.;
    page_ref2 = NULL;
    count2 = 0;
    do {
        ++count2;
        WT_WITH_DHANDLE(wt_session, dhandle,
          page_from_npos_fn(wt_session, &page_ref, npos, WT_READ_PREV, WT_READ_PREV));
        if (verbose > 1)
            printf("npos = %f, page_ref = %p\n", npos, (void *)page_ref);
        if (page_ref == NULL)
            break;

        if (in_mem)
            testutil_assertfmt(page_ref != page_ref2,
              "Got the same page twice: %p, npos = %lf, prev_npos = %lf", (void *)page_ref, npos,
              prev_npos);

        prev_npos = npos;
        page_ref2 = page_ref;
        npos = __wt_page_npos(wt_session, page_ref, -1e-5, NULL, NULL, 0);
        testutil_assertfmt(
          npos < prev_npos, "next npos(%lf) must be smaller than prev_npos(%lf)", npos, prev_npos);
        WT_WITH_DHANDLE(
          wt_session, dhandle, testutil_check(__wt_page_release(wt_session, page_ref, 0)));
    } while (npos > 0.0);
    if (verbose)
        printf("  ... %d\n", count2);
    if (in_mem)
        testutil_assertfmt(count2 == NUM_KEYS,
          "should have traversed %d pages, but only traversed %d", NUM_KEYS, count2);
    /* For on-disk database, there's no guarantee that it's one key per page */

    if (in_mem || page_from_npos_fn == __wt_page_from_npos_for_read)
        testutil_assertfmt(count1 == count2,
          "Number of pages traversed forward (%d) and backward (%d) don't match", count1, count2);

    /*
     * Traverse the whole dataset, checking npos.
     */
    if (verbose)
        printf("  keys\n");
    prev_npos = 0.;
    path_str[0][0] = path_str[1][0] = 0;
    count = 0;
    for (int key = 0; key < NUM_KEYS; key++, count++) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));

        path_str_offset = 0;
        page_ref = ((WT_CURSOR_BTREE *)cursor)->ref;

        /* Compute the soft position (npos) of the page */
        npos = __wt_page_npos(
          wt_session, page_ref, WT_NPOS_MID, path_str[count & 1], &path_str_offset, 1024);
        if (verbose > 1)
            printf("key %d: npos = %f, path_str = %s\n", key, npos, path_str[count & 1]);

        /* We're walking through all pages in order. Each page should have a larger or equal npos */
        testutil_assertfmt(npos >= prev_npos,
          "Page containing key %" PRIu64 " %s has npos (%f) smaller than the page of key %" PRIu64
          ", (%f) %s",
          key, path_str[count & 1], npos, key - 1, prev_npos, path_str[(count & 1) ^ 1]);
        prev_npos = npos;

        /* Now find which page npos restores to. We haven't modified the Btree so it should be the
         * exact same page */
        WT_WITH_DHANDLE(wt_session, dhandle,
          testutil_check(page_from_npos_fn(wt_session, &page_ref2, npos, 0, 0)));

        if (in_mem)
            testutil_assertfmt(page_ref == page_ref2,
              "page mismatch for key %llu!\n  Expected %p, got %p\n  npos = %f", key,
              (void *)page_ref, (void *)page_ref2, npos);

        /* __wt_page_from_npos sets a hazard pointer on the found page. Release it. */
        WT_WITH_DHANDLE(
          wt_session, dhandle, testutil_check(__wt_page_release(wt_session, page_ref2, 0)));
    }
    if (verbose)
        printf("  ... %d\n", count);

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
}

/*
 * position_key --
 *     Position the cursor at the given position with the given flags and return the record's key.
 */
static uint64_t
position_key(WT_CURSOR *cursor, double pos, uint32_t flags, WT_POSITION *p)
{
    uint64_t key;

    memset(p, 0, sizeof(*p));
    p->pos = pos;
    p->flags = flags;
    testutil_check(cursor->set_position(cursor, p));
    testutil_check(cursor->get_key(cursor, &key));
    return (key);
}

/*
 * page_start --
 *     Identify the cursor's page by the position of its start. Unlike the WT_REF pointer this is
 *     stable when an evicted internal page is read again.
 */
static double
page_start(WT_CURSOR_BTREE *cbt)
{
    return (__wt_page_npos(CUR2S(cbt), cbt->ref, 0., NULL, NULL, 0));
}

/*
 * check_page_edge --
 *     Check the cursor sits on the first (or last) indexed record of its page.
 */
static void
check_page_edge(WT_CURSOR_BTREE *cbt, bool first)
{
    WT_PAGE *page;

    page = cbt->ref->page;
    if (page->entries != 0) {
        testutil_assert(cbt->ins == NULL);
        testutil_assertfmt(cbt->slot == (first ? 0 : page->entries - 1),
          "slot %" PRIu32 " is not the %s of %" PRIu32 " entries", cbt->slot,
          first ? "first" : "last", page->entries);
    } else {
        /* Everything on the page is on the smallest-key insert list. */
        testutil_assert(cbt->ins_head == WT_ROW_INSERT_SMALLEST(page));
        testutil_assert(
          cbt->ins == (first ? WT_SKIP_FIRST(cbt->ins_head) : WT_SKIP_LAST(cbt->ins_head)));
    }
}

/*
 * test_cursor_position --
 *     Check WT_CURSOR::set_position and WT_CURSOR::get_position against the internal functions.
 *
 * NOTE!! This is a white box test. It uses functions and types not available in the WiredTiger API.
 */
static void
test_cursor_position(WT_CONNECTION *conn, bool in_mem)
{
    WT_CURSOR *cursor, *other;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;
    WT_ITEM before_raw, first_raw, sep_raw;
    WT_PAGE *page;
    WT_POSITION p;
    WT_REF *ref;
    WT_SESSION *session;
    WT_SESSION_IMPL *wt_session;
    double expect, hi, lo, page_id, pos, prev_pos;
    uint64_t exact, first, key, last, middle, near, prev_first, rt_key;
    int count, exact_cmp, i;
    char str_key[32];
    const char *str_boundary;
    bool have_before, on_disk;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, file_uri, NULL, NULL, &cursor));
    cbt = (WT_CURSOR_BTREE *)cursor;
    wt_session = (WT_SESSION_IMPL *)session;

    /* Walk everything once so the tree shape is stable for the rest of the test. */
    while ((ret = cursor->next(cursor)) == 0)
        ;
    testutil_assert(ret == WT_NOTFOUND);

    /* An unpositioned cursor has no position. */
    testutil_assert(cursor->get_position(cursor, &pos) == EINVAL);

    /*
     * Positions never decrease along a forward walk, lie inside the range of the record's page and,
     * for on-disk records, match the internal computation exactly.
     */
    if (verbose)
        printf("  get_position\n");
    prev_pos = -1.;
    count = 0;
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_position(cursor, &pos));
        testutil_check(cursor->get_key(cursor, &key));
        testutil_assertfmt(pos >= prev_pos,
          "key %" PRIu64 ": position %f smaller than the previous %f", key, pos, prev_pos);
        testutil_assert(pos >= 0. && pos <= 1.);

        lo = __wt_page_npos(wt_session, cbt->ref, 0., NULL, NULL, 0);
        hi = __wt_page_npos(wt_session, cbt->ref, 1., NULL, NULL, 0);
        testutil_assertfmt(lo <= pos && pos <= hi,
          "key %" PRIu64 ": position %f outside page range [%f, %f]", key, pos, lo, hi);
        if (cbt->ins == NULL) {
            expect = __wt_page_npos(
              wt_session, cbt->ref, (cbt->slot + 0.5) / cbt->ref->page->entries, NULL, NULL, 0);
            testutil_assert(DOUBLE_EQ(pos, expect));
        }
        prev_pos = pos;
        ++count;
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_assert(count == NUM_KEYS);

    /*
     * Round trip: positioning at a record's own position lands on its page, on the same record for
     * on-disk records and no later than it for insert list records.
     */
    if (verbose)
        printf("  round trip\n");
    for (key = 0; key < (uint64_t)NUM_KEYS; key += 13) {
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));
        on_disk = cbt->ins == NULL;
        page_id = page_start(cbt);
        testutil_check(cursor->get_position(cursor, &pos));

        memset(&p, 0, sizeof(p));
        p.pos = pos;
        testutil_check(cursor->set_position(cursor, &p));
        testutil_assert(p.pages_skipped == 0);
        testutil_check(cursor->get_key(cursor, &rt_key));
        testutil_assertfmt(DOUBLE_EQ(page_start(cbt), page_id),
          "key %" PRIu64 " round trip via %f changed page: %f to %f", key, pos, page_id,
          page_start(cbt));
        if (on_disk)
            testutil_assertfmt(rt_key == key, "key %" PRIu64 " round trip via %f returned %" PRIu64,
              key, pos, rt_key);
        else
            testutil_assert(rt_key <= key);
    }

    /*
     * Sample the tree at evenly spaced positions. For each position the anchors select the first,
     * middle and last records of one page, the samples arrive in key order, the key-only boundary
     * separates the page from its predecessor, and no page is skipped in an intact tree.
     */
    if (verbose)
        printf("  sampling\n");
    WT_CLEAR(before_raw);
    WT_CLEAR(first_raw);
    WT_CLEAR(sep_raw);
    prev_first = 0;
    for (i = 0; i <= NUM_SAMPLES; ++i) {
        pos = (double)i / NUM_SAMPLES;

        first = position_key(cursor, pos, WT_POSITION_ANCHOR_FIRST, &p);
        testutil_assert(p.pages_skipped == 0);
        ref = cbt->ref;
        page = ref->page;
        page_id = page_start(cbt);
        check_page_edge(cbt, true);
        testutil_check(__wt_buf_set(wt_session, &first_raw, cursor->key.data, cursor->key.size));
        if (i > 0)
            testutil_assertfmt(first >= prev_first,
              "position %f returned %" PRIu64 " after %" PRIu64, pos, first, prev_first);
        prev_first = first;

        /* The record before the first indexed one is on an earlier page or a pre-page insert. */
        have_before = false;
        ret = cursor->prev(cursor);
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == 0) {
            if (cbt->ref == ref)
                testutil_assert(cbt->ins != NULL && cbt->ins_head == WT_ROW_INSERT_SMALLEST(page));
            else {
                have_before = true;
                testutil_check(
                  __wt_buf_set(wt_session, &before_raw, cursor->key.data, cursor->key.size));
            }
        }

#define CHECK_SAME_PAGE(what)                                                                   \
    do {                                                                                        \
        testutil_assertfmt(p.pages_skipped == 0, "position %f (%s): skipped %" PRIu32 " pages", \
          pos, what, p.pages_skipped);                                                          \
        testutil_assertfmt(DOUBLE_EQ(page_start(cbt), page_id),                                 \
          "position %f (%s): page %f, expected %f", pos, what, page_start(cbt), page_id);       \
    } while (0)

        last = position_key(cursor, pos, WT_POSITION_ANCHOR_LAST | WT_POSITION_PREV, &p);
        CHECK_SAME_PAGE("last");
        check_page_edge(cbt, false);
        testutil_assert(last >= first);

        exact = position_key(cursor, pos, WT_POSITION_ANCHOR_EXACT, &p);
        CHECK_SAME_PAGE("exact");
        testutil_assert(first <= exact && exact <= last);

        middle = position_key(cursor, pos, WT_POSITION_ANCHOR_MIDDLE, &p);
        CHECK_SAME_PAGE("middle");
        testutil_assert(first <= middle && middle <= last);

        /* Unknown flag bits are ignored. */
        testutil_assert(
          position_key(cursor, pos, WT_POSITION_ANCHOR_FIRST | 0x80000000u, &p) == first);

        /* Cache-only positioning always succeeds when the whole tree is in memory. */
        if (in_mem) {
            testutil_assert(position_key(cursor, pos, WT_POSITION_CACHE_ONLY, &p) == exact);
            testutil_assert(p.pages_skipped == 0);
        }

        /* Key-only: the boundary key of the page, without a position. */
        memset(&p, 0, sizeof(p));
        p.pos = pos;
        p.flags = WT_POSITION_KEY_ONLY | WT_POSITION_ANCHOR_LAST | WT_POSITION_PREV;
        testutil_check(cursor->set_position(cursor, &p));
        testutil_assert(p.pages_skipped == 0);
        testutil_assert(cbt->ref == NULL && !F_ISSET(cbt, WT_CBT_ACTIVE));
        testutil_assert(F_ISSET(cursor, WT_CURSTD_KEY_EXT) && !F_ISSET(cursor, WT_CURSTD_KEY_INT));
        testutil_assert(cursor->get_position(cursor, &expect) == EINVAL);
        testutil_check(__wt_buf_set(wt_session, &sep_raw, cursor->key.data, cursor->key.size));

        if (sep_raw.size == 0)
            /* The leftmost page has the empty key as its boundary. */
            testutil_assertfmt(
              !have_before, "position %f: empty boundary key but a record precedes the page", pos);
        else {
            /* Boundary keys are order preserving on their packed form. */
            testutil_assert(__wt_lex_compare(&sep_raw, &first_raw) <= 0);
            if (have_before)
                testutil_assert(__wt_lex_compare(&sep_raw, &before_raw) > 0);

            /* Searching from the boundary key reaches the first record of the page. */
            testutil_check(cursor->search_near(cursor, &exact_cmp));
            testutil_check(cursor->get_key(cursor, &near));
            testutil_assertfmt(
              exact_cmp >= 0 && near == first && DOUBLE_EQ(page_start(cbt), page_id),
              "position %f: search_near from the boundary key returned %" PRIu64
              ", expected %" PRIu64,
              pos, near, first);
        }
    }

    /* Position 1 addresses the last page: only insert-list records can follow its last record. */
    last = position_key(cursor, 1., WT_POSITION_ANCHOR_LAST | WT_POSITION_PREV, &p);
    ref = cbt->ref;
    ret = cursor->next(cursor);
    testutil_assert(ret == WT_NOTFOUND || (cbt->ref == ref && cbt->ins != NULL));

    __wt_buf_free(wt_session, &before_raw);
    __wt_buf_free(wt_session, &first_raw);
    __wt_buf_free(wt_session, &sep_raw);

    /* Argument and cursor state errors. */
    if (verbose)
        printf("  errors\n");
    memset(&p, 0, sizeof(p));
    p.pos = strtod("NaN", NULL);
    testutil_assert(cursor->set_position(cursor, &p) == EINVAL);

    cursor->set_key(cursor, (uint64_t)10);
    testutil_check(cursor->bound(cursor, "bound=lower"));
    p.pos = 0.5;
    testutil_assert(cursor->set_position(cursor, &p) == EINVAL);
    testutil_check(cursor->bound(cursor, "action=clear"));
    testutil_check(cursor->reset(cursor));

    testutil_check(session->open_cursor(session, file_uri, NULL, "next_random=true", &other));
    testutil_assert(other->set_position(other, &p) == EINVAL);
    testutil_check(other->close(other));

    testutil_check(
      session->create(session, "file:normalized_pos_col.wt", "key_format=r,value_format=S"));
    testutil_check(session->open_cursor(session, "file:normalized_pos_col.wt", NULL, NULL, &other));
    testutil_assert(other->set_position(other, &p) == ENOTSUP);
    testutil_assert(other->get_position(other, &pos) == ENOTSUP);
    testutil_check(other->close(other));

    testutil_check(
      session->create(session, "file:normalized_pos_empty.wt", "key_format=Q,value_format=S"));
    if (!in_mem) {
        /* Bulk load is ignored by in-memory databases. */
        testutil_check(
          session->open_cursor(session, "file:normalized_pos_empty.wt", NULL, "bulk", &other));
        testutil_assert(other->set_position(other, &p) == EINVAL);
        testutil_check(other->close(other));
    }
    testutil_check(
      session->open_cursor(session, "file:normalized_pos_empty.wt", NULL, NULL, &other));
    testutil_assert(other->set_position(other, &p) == WT_NOTFOUND);
    p.flags = WT_POSITION_KEY_ONLY;
    testutil_check(other->set_position(other, &p));
    testutil_assert(other->key.size == 0);
    testutil_check(other->close(other));

    /*
     * On a string-keyed table the empty boundary reads as an empty C string, even after a non-empty
     * boundary has filled the cursor's key buffer.
     */
    testutil_check(session->create(session, "file:normalized_pos_str.wt",
      "key_format=S,value_format=S,memory_page_max=1KB,leaf_page_max=1KB,allocation_size=1KB"));
    testutil_check(session->open_cursor(session, "file:normalized_pos_str.wt", NULL, NULL, &other));
    for (i = 0; i < NUM_KEYS / 10; ++i) {
        testutil_snprintf(str_key, sizeof(str_key), "key%08d", i);
        other->set_key(other, str_key);
        other->set_value(other, str_key);
        testutil_check(other->insert(other));
    }
    /* Stabilize the tree shape so a key-only descent finds internal pages. */
    while ((ret = other->next(other)) == 0)
        ;
    testutil_assert(ret == WT_NOTFOUND);
    p.pos = 0.5;
    testutil_check(other->set_position(other, &p));
    testutil_assert(other->key.size > 0);
    testutil_check(other->get_key(other, &str_boundary));
    testutil_assertfmt(strlen(str_boundary) == other->key.size,
      "key-only boundary \"%s\" is not terminated at its %" WT_SIZET_FMT " bytes", str_boundary,
      other->key.size);
    p.pos = 0.;
    testutil_check(other->set_position(other, &p));
    testutil_check(other->get_key(other, &str_boundary));
    testutil_assertfmt(strlen(str_boundary) == 0,
      "leftmost key-only boundary on a string table is \"%s\", expected an empty string",
      str_boundary);
    testutil_check(other->close(other));

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
}

/*
 * test_cursor_position_skipped --
 *     Build a tree whose leaves can be fast-truncated, reopen the database so nothing is cached,
 *     fast-truncate a range of pages, and check that positioning into the range lands on the record
 *     adjacent to it in the walk direction and reports the pages it skipped. Also check cache-only
 *     positioning against a cold cache.
 */
static void
test_cursor_position_skipped(const char *home)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor, *start, *stop;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;
    WT_PAGE *parent;
    WT_POSITION p;
    WT_SESSION *session;
    double hi_pos, lo_pos, pos;
    uint64_t key, range_start, range_stop;
    int i, notfound, skipped;
    char val_str[100];
    static const char *trunc_uri = "file:normalized_pos_trunc.wt";

    /* Values small enough to stay on the page: pages with overflow items cannot be fast-deleted. */
    testutil_check(wiredtiger_open(home, NULL, "cache_size=100MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->create(
      session, trunc_uri, "key_format=Q,value_format=S,leaf_page_max=4KB,allocation_size=4KB"));
    testutil_check(session->open_cursor(session, trunc_uri, NULL, NULL, &cursor));
    memset(val_str, 'B', sizeof(val_str));
    val_str[sizeof(val_str) - 1] = '\0';
    for (key = 0; key < (uint64_t)NUM_KEYS; ++key) {
        cursor->set_key(cursor, key);
        cursor->set_value(cursor, val_str);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_check(session->checkpoint(session, NULL));
    testutil_check(conn->close(conn, ""));

    testutil_check(wiredtiger_open(home, NULL, "cache_size=100MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(session->open_cursor(session, trunc_uri, NULL, NULL, &cursor));
    cbt = (WT_CURSOR_BTREE *)cursor;

    /* Cache-only positioning against a cold cache never reads and never claims an exact hit. */
    notfound = skipped = 0;
    for (i = 0; i <= NUM_SAMPLES; ++i) {
        memset(&p, 0, sizeof(p));
        p.pos = (double)i / NUM_SAMPLES;
        p.flags = WT_POSITION_CACHE_ONLY;
        ret = cursor->set_position(cursor, &p);
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == WT_NOTFOUND)
            ++notfound;
        else if (p.pages_skipped != 0)
            ++skipped;
    }
    testutil_assert(notfound + skipped > 0);
    testutil_check(cursor->reset(cursor));

    /*
     * Choose a range of about a dozen pages whose boundary pages share a parent, so the live
     * neighbors of the range sit under the same internal page as the deleted pages. A position
     * between the range boundaries addresses a page inside the range.
     */
    for (range_start = 50000;; range_start += 1000) {
        testutil_assert(range_start < 90000);
        range_stop = range_start + 400;
        cursor->set_key(cursor, range_start);
        testutil_check(cursor->search(cursor));
        parent = cbt->ref->home;
        testutil_check(cursor->get_position(cursor, &lo_pos));
        cursor->set_key(cursor, range_stop);
        testutil_check(cursor->search(cursor));
        if (cbt->ref->home == parent)
            break;
    }
    testutil_check(cursor->get_position(cursor, &hi_pos));
    testutil_check(cursor->reset(cursor));
    testutil_assert(lo_pos < hi_pos);
    pos = (lo_pos + hi_pos) / 2;

    testutil_check(session->open_cursor(session, trunc_uri, NULL, NULL, &start));
    testutil_check(session->open_cursor(session, trunc_uri, NULL, NULL, &stop));
    start->set_key(start, range_start);
    stop->set_key(stop, range_stop);
    testutil_check(session->truncate(session, NULL, start, stop, NULL));
    testutil_check(start->close(start));
    testutil_check(stop->close(stop));

    /* Forwards lands on the first record after the range, backwards on the last before it. */
    memset(&p, 0, sizeof(p));
    p.pos = pos;
    testutil_check(cursor->set_position(cursor, &p));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_assertfmt(key == range_stop + 1 && p.pages_skipped > 0,
      "position %f after truncate returned %" PRIu64 " skipping %" PRIu32
      " pages, expected %" PRIu64,
      pos, key, p.pages_skipped, range_stop + 1);

    p.flags = WT_POSITION_PREV;
    testutil_check(cursor->set_position(cursor, &p));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_assertfmt(key == range_start - 1 && p.pages_skipped > 0,
      "position %f (prev) after truncate returned %" PRIu64 " skipping %" PRIu32
      " pages, expected %" PRIu64,
      pos, key, p.pages_skipped, range_start - 1);

    /* Key-only positioning is unaffected by the state of the leaves. */
    p.flags = WT_POSITION_KEY_ONLY;
    testutil_check(cursor->set_position(cursor, &p));
    testutil_assert(p.pages_skipped == 0);

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, ""));
    testutil_check(conn->close(conn, ""));
}

/*
 * run --
 *     Run the test.
 *
 * Create a btree with one key per page. Soft positions work on the in-memory btree, so use an
 *     in-memory version of WiredTiger to keep things simple when reasoning about the shape of the
 *     btree.
 *
 * Then, test that a computed npos returns to the same page it was derived from. This assumes no
 *     change of the underlying btree during the test.
 */
static void
run(const char *working_dir, bool in_mem)
{
    WT_CONNECTION *conn;
    char home[1024];

    testutil_work_dir_from_path(home, sizeof(home), working_dir);
    testutil_recreate_dir(home);

    testutil_check(wiredtiger_open(home, NULL,
      in_mem ? "create,in_memory=true,cache_size=1GB" : "create,cache_size=1MB", &conn));

    create_btree(conn);

    if (verbose)
        printf(" evict\n");

    test_normalized_pos(conn, in_mem, __wt_page_from_npos_for_eviction);

    if (verbose)
        printf(" read\n");

    test_normalized_pos(conn, in_mem, __wt_page_from_npos_for_read);

    if (verbose)
        printf(" cursor\n");

    test_cursor_position(conn, in_mem);

    testutil_check(conn->close(conn, ""));

    if (!in_mem) {
        if (verbose)
            printf(" skipped\n");
        test_cursor_position_skipped(home);
    }

    testutil_remove(home);
}

/*
 * main --
 *     Test correctness of normalized position.
 */
int
main(int argc, char *argv[])
{
    int ch;
    const char *working_dir;

    working_dir = "WT_TEST.normalized_pos";

    while ((ch = __wt_getopt(progname, argc, argv, "h:v")) != EOF)
        switch (ch) {
        case 'h':
            working_dir = __wt_optarg;
            break;
        case 'v':
            ++verbose;
            break;
        default:
            usage();
        }

    argc -= __wt_optind;
    if (argc != 0)
        usage();

    if (verbose)
        printf("mem\n");
    run(working_dir, true);
    if (verbose)
        printf("disk\n");
    run(working_dir, false);
    return 0;
}
