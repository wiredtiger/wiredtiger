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
 * call boundary).
 */

#include <stdio.h>
#include <stdlib.h>

/*
 * Used by ASSERT_FREE/check_free
 */
static FILE *tmpfp;

static const char *K1 = "key1";
static const char *K2 = "key2222";
static const char *K3 = "key333";

static const char *V1 = "value1";
static const char *V2 = "value2222";
static const char *V3 = "value333";

/*
 * Assert that the memory was freed at some point by the WiredTiger API, and if allocated and reused
 * since then, does not have the contents it previously held.
 */
#define ASSERT_FREE(mem, prev_mem) testutil_assert(check_free(mem, prev_mem))

/*
 * Asserts that the memory is valid and has the expected value.
 */
#define ASSERT_ALLOCED(mem, prev_mem) testutil_assert(!check_free(mem, prev_mem))

#define SAVE(save_array, save_ptr, p)                    \
    do {                                                 \
        size_t _len = strlen(p);                         \
        save_ptr = p;                                    \
        testutil_assert(_len + 1 <= sizeof(save_array)); \
        memcpy(save_array, p, _len + 1);                 \
    } while (0)

/*
 * Check if the memory has been freed, return 1 if so, otherwise zero. It is not possible to
 * portably tell if memory has been freed (as it may be reused by the allocator for something else),
 * so we instead check if the memory is valid, and if so, it does not contain the previously set
 * contents.
 */
int
check_free(void *mem, const char *prev_mem)
{
    size_t len, wrote;

    /*
     * Checking memory without faulting in user memory can be done via accessing the memory in a
     * system call. It's not fast, but it doesn't need to be.
     */
    len = strlen(prev_mem);
    errno = 0;
    wrote = fwrite(mem, 1, len, tmpfp);
    if (wrote == 0 && errno == EFAULT)
        return (1);
    testutil_assert(wrote == len);

    printf(" expect junk: %s (cannot be %s)\n", (const char *)mem, prev_mem);
    return (strncmp(mem, prev_mem, len) != 0);
}

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
    tmpfp = tmpfile();
    testutil_assert(tmpfp != NULL);

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
        if (strcmp(key, K3) == 0) {
            printf("GOT IT\n");
        }

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
    fclose(tmpfp);
    return (EXIT_SUCCESS);
}
