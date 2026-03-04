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

typedef struct arg_v2 {
    uint64_t version;
    /* version 1 */
    uint64_t *ref_1;
    int i_1;
    unsigned char uc_2;

    /* version 2*/
    uint64_t *ref_3;
    /* Reason to use bool is to test memory alignment. */
    bool b_4;
} ARG_V2;

/* The reason to use multi struct is to test the compiler consistency. */
typedef struct arg_v4 {
    uint64_t version;
    /* version 1 */
    uint64_t *ref_1;
    int i_1;
    unsigned char uc_2;

    /* version 2 */
    uint64_t *ref_3;
    bool b_4;

    /* version 3 */
    unsigned char uc_5;
    const char *str_6;

    /* version 4 */
    bool b_7;
} ARG_V4;

static const char demo_str[] = "This is a demo";
static uint64_t demo_ref = 0xCCCCCCCCCCCCCCCC;

static void decode_verify(ARG_V4 *arg);
static void refill_v2(ARG_V2 *arg, uint64_t version);
static void refill_v4(ARG_V4 *arg, uint64_t version);
static void test_for_upgrade(void);

/*
 * decode_verify --
 *     Verify the decoded arguments.
 */
static void
decode_verify(ARG_V4 *arg)
{
    if (arg == NULL)
        return;
    if (arg->version >= 1) {
        /* Version 1 available */
        testutil_assert(arg->ref_1 == &demo_ref);
        testutil_assert(arg->i_1 == 1);
        testutil_assert(arg->uc_2 == 2);
    }
    if (arg->version >= 2) {
        /* Version 2 available */
        testutil_assert(arg->ref_3 == &demo_ref);
        testutil_assert(arg->b_4 == true);
    }
    if (arg->version >= 3) {
        /* Version 3 available */
        testutil_assert(arg->uc_5 == 5);
        testutil_assert(strcmp(arg->str_6, demo_str) == 0);
    }
    if (arg->version >= 4) {
        /* Version 4 available */
        testutil_assert(arg->b_7 == true);
    }
}

/*
 * refill_v2 --
 *     Refill the arguments for version 2.
 */
static void
refill_v2(ARG_V2 *arg, uint64_t version)
{
    /* Reason to not reuse refill_v4 is to avoid miss memory layout check. */
    if (arg == NULL)
        return;
    WT_CLEAR(*arg);
    arg->version = version;
    if (version >= 1) {
        /* Version 1 available */
        arg->ref_1 = &demo_ref;
        arg->i_1 = 1;
        arg->uc_2 = 2;
    }
    if (version >= 2) {
        /* Version 2 available */
        arg->ref_3 = &demo_ref;
        arg->b_4 = true;
    }
}

/*
 * refill_v4 --
 *     Refill the arguments for version 4.
 */
static void
refill_v4(ARG_V4 *arg, uint64_t version)
{
    /* Reason to not reuse refill_v2 is to avoid miss memory layout check. */
    if (arg == NULL)
        return;
    WT_CLEAR(*arg);
    arg->version = version;
    if (version >= 1) {
        /* Version 1 available */
        arg->ref_1 = &demo_ref;
        arg->i_1 = 1;
        arg->uc_2 = 2;
    }
    if (version >= 2) {
        /* Version 2 available */
        arg->ref_3 = &demo_ref;
        arg->b_4 = true;
    }
    if (version >= 3) {
        /* Version 3 available */
        arg->uc_5 = 5;
        arg->str_6 = demo_str;
    }
    if (version >= 4) {
        /* Version 4 available */
        arg->b_7 = true;
    }
}

/*
 * test_for_upgrade --
 *     Test the upgrade version correctness.
 */
static void
test_for_upgrade(void)
{
    ARG_V2 arg_2;
    ARG_V4 arg_4;
    for (uint64_t version = 1; version <= 2; version++) {
        refill_v2(&arg_2, version);
        decode_verify((ARG_V4 *)&arg_2);
    }
    for (uint64_t version = 1; version <= 4; version++) {
        refill_v4(&arg_4, version);
        decode_verify(&arg_4);
    }
}

/*
 * main --
 *     The main method.
 */
int
main(int argc, char *argv[])
{
    WT_UNUSED(testutil_set_progname(argv));
    WT_UNUSED(argc);
    test_for_upgrade();
    return (EXIT_SUCCESS);
}
