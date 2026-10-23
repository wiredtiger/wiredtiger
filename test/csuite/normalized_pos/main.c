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

/* Key formats exercised in key-only mode. */
typedef enum {
    FMT_q,
    FMT_Q,
    FMT_i,
    FMT_S,
    FMT_u,
    FMT_qS,
    FMT_Sq,
    FMT_qu,
    FMT_10uq,
    FMT_COUNT
} FMT_ID;
static const char *const fmt_strings[FMT_COUNT] = {
  "q", "Q", "i", "S", "u", "qS", "Sq", "qu", "10uq"};
static const int NUM_FMT_KEYS = 20000;

/* An unpacked key: up to two fields, each tagged with its format character. */
typedef struct {
    char type;
    int64_t i;
    uint64_t u;
    char s[32];
    uint8_t raw[32];
    size_t rawsize;
} FMT_FIELD;
typedef struct {
    FMT_FIELD f[2];
    int nfields;
} FMT_KEY;

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
 * position_flags --
 *     Position the cursor at the given position with the given flags.
 */
static int
position_flags(WT_CURSOR *cursor, double pos, uint32_t flags)
{
    WT_POSITION p;

    memset(&p, 0, sizeof(p));
    p.pos = pos;
    p.flags = flags;
    return (cursor->set_position(cursor, &p));
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
    /* A completed boundary is a packed string: its NUL terminator is part of the key. */
    testutil_assertfmt(strlen(str_boundary) + 1 == other->key.size,
      "key-only boundary \"%s\" is not a packed string of %" WT_SIZET_FMT " bytes", str_boundary,
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
 * fmt_insert --
 *     Insert record i into a table of the given key format.
 */
static void
fmt_insert(WT_CURSOR *cursor, FMT_ID id, int i)
{
    WT_ITEM item;
    int64_t q;
    char str[32], value[80];

    q = (int64_t)(i - NUM_FMT_KEYS / 2) * 1000003;
    switch (id) {
    case FMT_q:
        cursor->set_key(cursor, q);
        break;
    case FMT_Q:
        cursor->set_key(cursor, (uint64_t)i * 7919);
        break;
    case FMT_i:
        cursor->set_key(cursor, (int32_t)((i - NUM_FMT_KEYS / 2) * 3));
        break;
    case FMT_S:
        testutil_snprintf(str, sizeof(str), "key%08d", i);
        cursor->set_key(cursor, str);
        break;
    case FMT_u:
        testutil_snprintf(str, sizeof(str), "key%08d", i);
        item.data = str;
        item.size = strlen(str);
        cursor->set_key(cursor, &item);
        break;
    case FMT_qS:
        testutil_snprintf(str, sizeof(str), "k%08d", i);
        cursor->set_key(cursor, (int64_t)(i / 7 - NUM_FMT_KEYS / 14) * 1000003, str);
        break;
    case FMT_Sq:
        testutil_snprintf(str, sizeof(str), "k%04d", i / 7);
        cursor->set_key(cursor, str, (int64_t)i);
        break;
    case FMT_qu:
        testutil_snprintf(str, sizeof(str), "k%08d", i);
        item.data = str;
        item.size = strlen(str);
        cursor->set_key(cursor, (int64_t)(i / 7 - NUM_FMT_KEYS / 14), &item);
        break;
    case FMT_10uq:
        testutil_snprintf(str, sizeof(str), "k%04d", i / 7);
        item.data = str;
        item.size = strlen(str);
        cursor->set_key(cursor, &item, (int64_t)i);
        break;
    case FMT_COUNT:
        testutil_assert(false);
    }
    memset(value, 'v', sizeof(value));
    value[sizeof(value) - 1] = '\0';
    cursor->set_value(cursor, value);
    testutil_check(cursor->insert(cursor));
}

/*
 * fmt_get_key --
 *     Unpack the cursor's key through the public API into fields that outlive the next call.
 */
static void
fmt_get_key(WT_CURSOR *cursor, FMT_ID id, FMT_KEY *key)
{
    WT_ITEM item;
    int32_t i32;
    const char *str;

    memset(key, 0, sizeof(*key));
    key->nfields = 1;
    switch (id) {
    case FMT_q:
        key->f[0].type = 'q';
        testutil_check(cursor->get_key(cursor, &key->f[0].i));
        break;
    case FMT_Q:
        key->f[0].type = 'Q';
        testutil_check(cursor->get_key(cursor, &key->f[0].u));
        break;
    case FMT_i:
        key->f[0].type = 'i';
        testutil_check(cursor->get_key(cursor, &i32));
        key->f[0].i = i32;
        break;
    case FMT_S:
        key->f[0].type = 'S';
        testutil_check(cursor->get_key(cursor, &str));
        testutil_snprintf(key->f[0].s, sizeof(key->f[0].s), "%s", str);
        break;
    case FMT_u:
        key->f[0].type = 'u';
        testutil_check(cursor->get_key(cursor, &item));
        testutil_assert(item.size <= sizeof(key->f[0].raw));
        memcpy(key->f[0].raw, item.data, item.size);
        key->f[0].rawsize = item.size;
        break;
    case FMT_qS:
        key->nfields = 2;
        key->f[0].type = 'q';
        key->f[1].type = 'S';
        testutil_check(cursor->get_key(cursor, &key->f[0].i, &str));
        testutil_snprintf(key->f[1].s, sizeof(key->f[1].s), "%s", str);
        break;
    case FMT_Sq:
        key->nfields = 2;
        key->f[0].type = 'S';
        key->f[1].type = 'q';
        testutil_check(cursor->get_key(cursor, &str, &key->f[1].i));
        testutil_snprintf(key->f[0].s, sizeof(key->f[0].s), "%s", str);
        break;
    case FMT_qu:
        key->nfields = 2;
        key->f[0].type = 'q';
        key->f[1].type = 'u';
        testutil_check(cursor->get_key(cursor, &key->f[0].i, &item));
        testutil_assert(item.size <= sizeof(key->f[1].raw));
        memcpy(key->f[1].raw, item.data, item.size);
        key->f[1].rawsize = item.size;
        break;
    case FMT_10uq:
        key->nfields = 2;
        key->f[0].type = 'u';
        key->f[1].type = 'q';
        testutil_check(cursor->get_key(cursor, &item, &key->f[1].i));
        testutil_assert(item.size == 10);
        memcpy(key->f[0].raw, item.data, item.size);
        key->f[0].rawsize = item.size;
        break;
    case FMT_COUNT:
        testutil_assert(false);
    }
}

/*
 * fmt_cmp --
 *     Compare two unpacked keys the way their format orders them.
 */
static int
fmt_cmp(const FMT_KEY *a, const FMT_KEY *b)
{
    const FMT_FIELD *fa, *fb;
    size_t len;
    int cmp, n;

    for (n = 0; n < a->nfields; ++n) {
        fa = &a->f[n];
        fb = &b->f[n];
        switch (fa->type) {
        case 'q':
        case 'i':
            cmp = fa->i < fb->i ? -1 : fa->i > fb->i ? 1 : 0;
            break;
        case 'Q':
            cmp = fa->u < fb->u ? -1 : fa->u > fb->u ? 1 : 0;
            break;
        case 'S':
            cmp = strcmp(fa->s, fb->s);
            break;
        case 'u':
            len = WT_MIN(fa->rawsize, fb->rawsize);
            cmp = memcmp(fa->raw, fb->raw, len);
            if (cmp == 0)
                cmp = fa->rawsize < fb->rawsize ? -1 : fa->rawsize > fb->rawsize ? 1 : 0;
            break;
        default:
            testutil_assert(false);
        }
        if (cmp != 0)
            return (cmp);
    }
    return (0);
}

/*
 * test_cursor_position_formats --
 *     Key-only boundaries of tables with typed and multi-column key formats unpack through the
 *     public API and separate the page from the records before it in the format's own order. A
 *     table with a custom collator returns the separator as stored.
 */
static void
test_cursor_position_formats(const char *home, const char *build_dir)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_ITEM first_raw;
    WT_SESSION *session;
    WT_SESSION_IMPL *wt_session;
    FMT_KEY before, boundary, first;
    double pos;
    int64_t q, q_first;
    int exact_cmp, i, id;
    char config[1024], table_config[128], fmt_uri[64];
    bool have_before;
    static const char *const rev_uri = "file:normalized_pos_rev.wt";

    if (build_dir != NULL)
        testutil_snprintf(config, sizeof(config),
          "create,cache_size=100MB,extensions=[%s/ext/collators/reverse/"
          "libwiredtiger_reverse_collator.so]",
          build_dir);
    else
        testutil_snprintf(config, sizeof(config), "create,cache_size=100MB");

    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    for (id = 0; id < FMT_COUNT; ++id) {
        testutil_snprintf(
          fmt_uri, sizeof(fmt_uri), "file:normalized_pos_fmt_%s.wt", fmt_strings[id]);
        testutil_snprintf(table_config, sizeof(table_config),
          "key_format=%s,value_format=S,leaf_page_max=4KB,internal_page_max=4KB", fmt_strings[id]);
        testutil_check(session->create(session, fmt_uri, table_config));
        testutil_check(session->open_cursor(session, fmt_uri, NULL, NULL, &cursor));
        for (i = 0; i < NUM_FMT_KEYS; ++i)
            fmt_insert(cursor, (FMT_ID)id, i);
        testutil_check(cursor->close(cursor));
    }
    if (build_dir != NULL) {
        testutil_check(session->create(session, rev_uri,
          "key_format=q,value_format=S,collator=reverse,leaf_page_max=4KB,internal_page_max=4KB"));
        testutil_check(session->open_cursor(session, rev_uri, NULL, NULL, &cursor));
        for (i = 0; i < NUM_FMT_KEYS; ++i)
            fmt_insert(cursor, FMT_q, i);
        testutil_check(cursor->close(cursor));
    }
    testutil_check(session->checkpoint(session, NULL));
    testutil_check(conn->close(conn, ""));

    /* Read the trees back from disk, with the separators reconciliation wrote. */
    testutil_check(wiredtiger_open(home, NULL, config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    wt_session = (WT_SESSION_IMPL *)session;

    for (id = 0; id < FMT_COUNT; ++id) {
        if (verbose)
            printf("  format %s\n", fmt_strings[id]);
        testutil_snprintf(
          fmt_uri, sizeof(fmt_uri), "file:normalized_pos_fmt_%s.wt", fmt_strings[id]);
        testutil_check(session->open_cursor(session, fmt_uri, NULL, NULL, &cursor));
        for (i = 0; i <= NUM_SAMPLES; ++i) {
            pos = (double)i / NUM_SAMPLES;

            testutil_check(position_flags(cursor, pos, WT_POSITION_ANCHOR_FIRST));
            fmt_get_key(cursor, (FMT_ID)id, &first);
            have_before = cursor->prev(cursor) == 0;
            if (have_before)
                fmt_get_key(cursor, (FMT_ID)id, &before);

            /* The leftmost page's boundary is the empty sentinel whatever the format. */
            testutil_check(position_flags(cursor, pos, WT_POSITION_KEY_ONLY));
            if (i == 0)
                testutil_assert(cursor->key.size == 0);
            if (cursor->key.size == 0) {
                testutil_assertfmt(!have_before,
                  "format %s position %f: empty boundary but a record precedes the page",
                  fmt_strings[id], pos);
                continue;
            }

            /* The boundary unpacks and separates the page from what precedes it. */
            fmt_get_key(cursor, (FMT_ID)id, &boundary);
            testutil_assertfmt(fmt_cmp(&boundary, &first) <= 0,
              "format %s position %f: boundary sorts after the page's first key", fmt_strings[id],
              pos);
            if (have_before)
                testutil_assertfmt(fmt_cmp(&before, &boundary) < 0,
                  "format %s position %f: boundary does not sort after the previous page",
                  fmt_strings[id], pos);

            testutil_check(cursor->search_near(cursor, &exact_cmp));
            fmt_get_key(cursor, (FMT_ID)id, &boundary);
            testutil_assert(exact_cmp >= 0 && fmt_cmp(&boundary, &first) == 0);
        }
        testutil_check(cursor->close(cursor));
    }

    if (build_dir != NULL) {
        if (verbose)
            printf("  reverse collator\n");
        WT_CLEAR(first_raw);
        testutil_check(session->open_cursor(session, rev_uri, NULL, NULL, &cursor));
        for (i = 0; i <= NUM_SAMPLES; ++i) {
            pos = (double)i / NUM_SAMPLES;
            testutil_check(position_flags(cursor, pos, WT_POSITION_ANCHOR_FIRST));
            testutil_check(cursor->get_key(cursor, &q_first));
            testutil_check(
              __wt_buf_set(wt_session, &first_raw, cursor->key.data, cursor->key.size));

            /* With a collator separators are never shortened: the boundary is the first key. */
            testutil_check(position_flags(cursor, pos, WT_POSITION_KEY_ONLY));
            if (i == 0)
                testutil_assert(cursor->key.size == 0);
            if (cursor->key.size == 0)
                continue;
            testutil_check(cursor->get_key(cursor, &q));
            testutil_assert(q == q_first);
            testutil_assert(cursor->key.size == first_raw.size &&
              memcmp(cursor->key.data, first_raw.data, first_raw.size) == 0);
        }
        __wt_buf_free(wt_session, &first_raw);
        testutil_check(cursor->close(cursor));
    }

    testutil_check(conn->close(conn, ""));
}

/*
 * check_separator --
 *     Check the stored separator of the page holding a key is the given single byte: the cut right
 *     after the first byte of an encoding that completion has to handle. A page in slot 0 of its
 *     parent carries no separator of its own, so the check only applies when the page has one.
 */
static void
check_separator(WT_CURSOR *cursor, uint8_t expected)
{
    WT_CURSOR_BTREE *cbt;
    WT_PAGE_INDEX *pindex;
    WT_SESSION_IMPL *wt_session;
    size_t size;
    uint32_t slot;
    const uint8_t *data;

    cbt = (WT_CURSOR_BTREE *)cursor;
    wt_session = CUR2S(cbt);
    testutil_check(cursor->search(cursor));
    WT_WITH_PAGE_INDEX(wt_session, __wt_ref_index_slot(wt_session, cbt->ref, &pindex, &slot));
    if (slot == 0) {
        printf(
          "page of the pinned edge key is the first child of its parent, separator not "
          "checked\n");
        return;
    }
    __wt_ref_key_home(cbt->ref, &data, &size);
    testutil_assertfmt(size == 1 && data[0] == expected,
      "separator is %" WT_SIZET_FMT " bytes starting 0x%02x, expected the single byte 0x%02x", size,
      size == 0 ? 0 : data[0], expected);
}

/*
 * check_int_edges --
 *     Walk a table of consecutive integer keys with one record per page and check every page's
 *     boundary against its first key and the record before it.
 */
static void
check_int_edges(WT_SESSION *session, const char *table_uri, bool is_signed)
{
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;
    WT_POSITION p;
    double pos;
    uint64_t boundary_u, key_u, prev_u;
    int64_t boundary_q, key_q, prev_q;
    bool first_on_page, have_prev;

    testutil_check(session->open_cursor(session, table_uri, NULL, NULL, &cursor));
    cbt = (WT_CURSOR_BTREE *)cursor;
    have_prev = false;
    prev_q = 0;
    prev_u = 0;
    while ((ret = cursor->next(cursor)) == 0) {
        if (is_signed)
            testutil_check(cursor->get_key(cursor, &key_q));
        else
            testutil_check(cursor->get_key(cursor, &key_u));
        first_on_page = cbt->slot == 0 && cbt->ins == NULL;
        testutil_check(cursor->get_position(cursor, &pos));

        memset(&p, 0, sizeof(p));
        p.pos = pos;
        p.flags = WT_POSITION_KEY_ONLY;
        testutil_check(cursor->set_position(cursor, &p));
        if (cursor->key.size != 0) {
            if (is_signed) {
                testutil_check(cursor->get_key(cursor, &boundary_q));
                testutil_assertfmt(boundary_q <= key_q,
                  "boundary %" PRId64 " is above record %" PRId64, boundary_q, key_q);
                if (first_on_page && have_prev)
                    testutil_assertfmt(boundary_q > prev_q,
                      "boundary %" PRId64 " is not above the previous page's %" PRId64, boundary_q,
                      prev_q);
            } else {
                testutil_check(cursor->get_key(cursor, &boundary_u));
                testutil_assertfmt(boundary_u <= key_u,
                  "boundary %" PRIu64 " is above record %" PRIu64, boundary_u, key_u);
                if (first_on_page && have_prev)
                    testutil_assertfmt(boundary_u > prev_u,
                      "boundary %" PRIu64 " is not above the previous page's %" PRIu64, boundary_u,
                      prev_u);
            }
        }

        /* Key-only positioning dropped the position: return to the record and move on. */
        if (is_signed) {
            cursor->set_key(cursor, key_q);
            prev_q = key_q;
        } else {
            cursor->set_key(cursor, key_u);
            prev_u = key_u;
        }
        testutil_check(cursor->search(cursor));
        have_prev = true;
    }
    testutil_assert(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
}

/*
 * test_cursor_position_int_edges --
 *     Consecutive integer keys spanning the values where the packed integer encoding changes
 *     length, so separators are cut right after the first byte of an encoding. Values just under
 *     the overflow threshold fill a 1KB page on their own, so the checkpointed tree has one record
 *     per page and a boundary at every key.
 */
static void
test_cursor_position_int_edges(const char *home)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    int64_t q;
    char val_str[500];
    static const char *const q_uri = "file:normalized_pos_edge_q.wt";
    static const char *const u_uri = "file:normalized_pos_edge_Q.wt";
    static const char *const table_config =
      "value_format=S,leaf_page_max=1KB,leaf_value_max=600,allocation_size=1KB";
    char config[128];

    memset(val_str, 'A', sizeof(val_str));
    val_str[sizeof(val_str) - 1] = '\0';

    testutil_check(wiredtiger_open(home, NULL, "create,cache_size=100MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    testutil_snprintf(config, sizeof(config), "key_format=Q,%s", table_config);
    testutil_check(session->create(session, u_uri, config));
    testutil_check(session->open_cursor(session, u_uri, NULL, NULL, &cursor));
    for (q = 8000; q <= 9000; ++q) {
        cursor->set_key(cursor, (uint64_t)q);
        cursor->set_value(cursor, val_str);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    testutil_snprintf(config, sizeof(config), "key_format=q,%s", table_config);
    testutil_check(session->create(session, q_uri, config));
    testutil_check(session->open_cursor(session, q_uri, NULL, NULL, &cursor));
    for (q = -9000; q <= 9000; q = q == -8000 ? 8000 : q + 1) {
        cursor->set_key(cursor, q);
        cursor->set_value(cursor, val_str);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    testutil_check(session->checkpoint(session, NULL));
    testutil_check(conn->close(conn, ""));

    /* The tree read back from disk does not change under the checks. */
    testutil_check(wiredtiger_open(home, NULL, "cache_size=100MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /*
     * 8256 is the first value with the positive multi-byte marker and 8255 the last two-byte one;
     * -8256 is the first two-byte negative value and -8257 the last multi-byte one. Both separators
     * are cut to the marker byte alone.
     */
    testutil_check(session->open_cursor(session, u_uri, NULL, NULL, &cursor));
    cursor->set_key(cursor, (uint64_t)8256);
    check_separator(cursor, 0xe1);
    testutil_check(cursor->close(cursor));
    testutil_check(session->open_cursor(session, q_uri, NULL, NULL, &cursor));
    cursor->set_key(cursor, (int64_t)-8256);
    check_separator(cursor, 0x20);
    testutil_check(cursor->close(cursor));

    check_int_edges(session, u_uri, false);
    check_int_edges(session, q_uri, true);
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
run(const char *working_dir, const char *build_dir, bool in_mem)
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
        if (verbose)
            printf(" formats\n");
        testutil_recreate_dir(home);
        test_cursor_position_formats(home, build_dir);
        if (verbose)
            printf(" integer edges\n");
        testutil_recreate_dir(home);
        test_cursor_position_int_edges(home);
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
    TEST_OPTS opts;
    int ch;
    const char *working_dir;

    working_dir = "WT_TEST.normalized_pos";

    /* The reverse collator extension is loaded from the build tree when it can be located. */
    memset(&opts, 0, sizeof(opts));
    opts.argv0 = argv[0];
    testutil_deduce_build_dir(&opts);
    if (opts.build_dir == NULL)
        printf("build directory not found from %s: skipping the collator check\n", argv[0]);

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
    run(working_dir, opts.build_dir, true);
    if (verbose)
        printf("disk\n");
    run(working_dir, opts.build_dir, false);
    free(opts.build_dir);
    return 0;
}
