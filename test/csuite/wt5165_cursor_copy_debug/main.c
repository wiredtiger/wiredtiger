/*-
 * Public Domain 2014-2019 MongoDB, Inc.
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
 * JIRA ticket reference: WT-5165 Test case description: Using the cursor copy debug mode, verify
 * that keys and values returned by API calls are freed as appropriate (typically at the next API
 * call boundary). Because it is not possible to portably tell if memory has been freed (as it may
 * be reused by the allocator for something else), we instead make sure it does not have the
 * previously set key or value in it. We do rely on the fact that after memory is freed, it remains
 * a valid memory address, but is either on the memory free list, or has been reused.
 */

#include <stdio.h>
#include <stdlib.h>

static const char *K1 = "key1";
static const char *K2 = "key2222";
static const char *K3 = "key333";

static const char *V1 = "value1";
static const char *V2 = "value2222";
static const char *V3 = "value333";

/*
 * We expect that the memory was freed at some point by the WiredTiger API. However, that same
 * memory may have been reused for another purpose. The best we can do is verify that either the
 * memory is marked free by malloc, or if not, that it has been overwritten with some other value.
 */
#define ASSERT_FREE(mem, prev_mem)                       \
    do {                                                 \
        testutil_assert(strcmp(mem, prev_mem) != 0);     \
        printf(" expect junk: %s\n", (const char *)mem); \
    } while (0)

#define ASSERT_ALLOCED(mem, prev_mem) testutil_assert(strcmp(mem, prev_mem) == 0)

#define SAVE(save_array, save_ptr, p)               \
    do {                                            \
        save_ptr = p;                               \
        strncpy(save_array, p, sizeof(save_array)); \
    } while (0)

int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    int direction, ret;
    char *key, *oldkey, *oldvalue, *value;
    char *kstr;
    char memkey[100], saved_oldkey[100], saved_oldvalue[100];

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_make_work_dir(opts->home);

    testutil_check(
      wiredtiger_open(opts->home, NULL, "create,debug_mode=(cursor_copy=true)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));
    testutil_check(session->create(session, opts->uri, "key_format=S,value_format=S"));

    testutil_check(session->open_cursor(session, opts->uri, NULL, NULL, &cursor));
    cursor->set_key(cursor, K1);
    cursor->set_value(cursor, V1);
    testutil_check(cursor->insert(cursor));
    cursor->set_key(cursor, K2);
    cursor->set_value(cursor, V2);
    testutil_check(cursor->insert(cursor));
    cursor->set_key(cursor, K3);
    cursor->set_value(cursor, V3);
    testutil_check(cursor->insert(cursor));

    /* Verify memory check macros */
    kstr = (char *)malloc(10);
    strncpy(kstr, K1, 10);
    printf(" k=%s\n", kstr);
    ASSERT_ALLOCED(kstr, K1);
    free(kstr);
    ASSERT_FREE(kstr, K1);

    printf("\n*FORWARD TEST*\n");
    oldkey = oldvalue = NULL;
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        testutil_check(cursor->get_value(cursor, &value));

        if (oldkey != NULL) {
            ASSERT_FREE(oldkey, saved_oldkey);
            ASSERT_FREE(oldvalue, saved_oldvalue);
        }
        printf("Got record: %s : %s\n", key, value);
        SAVE(saved_oldkey, oldkey, key);
        SAVE(saved_oldvalue, oldvalue, value);
    }
    if (oldkey != NULL) {
        ASSERT_FREE(oldkey, saved_oldkey);
        ASSERT_FREE(oldvalue, saved_oldvalue);
    }
    testutil_assert(ret == WT_NOTFOUND); /* Check for end-of-table. */

    printf("\n*BACKWARD TEST*\n");
    testutil_check(cursor->reset(cursor)); /* Restart the scan. */
    oldkey = oldvalue = NULL;
    while ((ret = cursor->prev(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &key));
        testutil_check(cursor->get_value(cursor, &value));

        if (oldkey != NULL) {
            ASSERT_FREE(oldkey, saved_oldkey);
            ASSERT_FREE(oldvalue, saved_oldvalue);
        }
        printf("Got record: %s : %s\n", key, value);
        SAVE(saved_oldkey, oldkey, key);
        SAVE(saved_oldvalue, oldvalue, value);
    }
    if (oldkey != NULL) {
        ASSERT_FREE(oldkey, saved_oldkey);
        ASSERT_FREE(oldvalue, saved_oldvalue);
    }
    testutil_assert(ret == WT_NOTFOUND); /* Check for end-of-table. */

    printf("\n*RESET TEST*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to last key */
    testutil_check(cursor->prev(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got last record: %s : %s\n", key, value);
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    testutil_check(cursor->reset(cursor));
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SET_KEY/VALUE TEST*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to last key */
    testutil_check(cursor->prev(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got last record: %s : %s\n", key, value);
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    cursor->set_key(cursor, "XXX");
    ASSERT_FREE(oldkey, saved_oldkey);
    cursor->set_value(cursor, "XXX");
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SEARCH TEST*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to first key */
    testutil_check(cursor->next(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    cursor->set_key(cursor, K2);
    testutil_check(cursor->search(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got record: %s : %s\n", key, value);
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SEARCH TEST REDO*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to first key */
    testutil_check(cursor->next(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    strncpy(memkey, K3, sizeof(memkey));
    cursor->set_key(cursor, (const char *)memkey);
    testutil_check(cursor->search(cursor));
    strncpy(memkey, "My memory", sizeof(memkey));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got record: %s : %s\n", key, value);
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SEARCH NOTFOUND TEST*\n");
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    cursor->set_key(cursor, "does not exist");
    if (cursor->search(cursor) != WT_NOTFOUND)
        abort();
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SEARCH_NEAR TEST*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to first key */
    testutil_check(cursor->next(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    cursor->set_key(cursor, "key2");
    testutil_check(cursor->search_near(cursor, &direction));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got record: %s : %s\n", key, value);
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    printf("\n*SEARCH_NEAR TEST REDO*\n");
    testutil_check(cursor->reset(cursor)); /* Start fresh, go to first key */
    testutil_check(cursor->next(cursor));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    SAVE(saved_oldkey, oldkey, key);
    SAVE(saved_oldvalue, oldvalue, value);
    strncpy(memkey, "key2", sizeof(memkey));
    cursor->set_key(cursor, (const char *)memkey);
    testutil_check(cursor->search_near(cursor, &direction));
    strncpy(memkey, "My memory", sizeof(memkey));
    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    printf("Got record: %s : %s\n", key, value);
    ASSERT_FREE(oldkey, saved_oldkey);
    ASSERT_FREE(oldvalue, saved_oldvalue);

    testutil_check(cursor->close(cursor));
    testutil_check(session->close(session, NULL));
    printf("Success\n");
    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
