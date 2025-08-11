/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-present WiredTiger, Inc.
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

/* Helpers for output formatting and size checks */

/*
 * print_hex_bytes --
 *     Print a buffer as space-separated hex bytes.
 */
static inline void
print_hex_bytes(const uint8_t *buf, size_t used)
{
    for (size_t k = 0; k < used; ++k)
        printf("%s%02x", k ? " " : "", buf[k]);
}

/*
 * print_bin_bytes_spaced --
 *     Print a buffer as space-separated binary bytes (MSB-first).
 */
static inline void
print_bin_bytes_spaced(const uint8_t *buf, size_t used)
{
    for (size_t k = 0; k < used; ++k) {
        putchar(' ');
        for (int b = 7; b >= 0; --b)
            putchar(((buf[k] >> b) & 1) ? '1' : '0');
    }
}

/*
 * print_hex_bin_columns --
 *     Print hex bytes and a padded binary column to align table output.
 */
static inline void
print_hex_bin_columns(const uint8_t *buf, size_t used)
{
    print_hex_bytes(buf, used);
    /* Pad to align the Bin column similarly to %-20s and %-30s headings */
    printf("%*s", (int)(21 - (used ? (3 * used - 1) : 0)), "");
    print_bin_bytes_spaced(buf, used);
}

/*
 * print_u64_array --
 *     Print an array of uint64_t values as [a, b, c].
 */
static inline void
print_u64_array(const uint64_t *arr, size_t n)
{
    printf("[");
    for (size_t j = 0; j < n; ++j)
        printf("%s%" PRIu64, j ? ", " : "", arr[j]);
    printf("]");
}

/*
 * print_hex_dump --
 *     Print a hex dump with a trailing byte count.
 */
static inline void
print_hex_dump(const uint8_t *buf, size_t used)
{
    printf("Hex dump: ");
    print_hex_bytes(buf, used);
    printf("\t(%" WT_SIZET_FMT " bytes)\n", used);
}

/*
 * print_bin_dump --
 *     Print a binary dump of a buffer, bytes separated by spaces.
 */
static inline void
print_bin_dump(const uint8_t *buf, size_t used)
{
    printf("Bin dump: ");
    for (size_t k = 0; k < used; ++k) {
        for (int b = 7; b >= 0; --b)
            putchar(((buf[k] >> b) & 1) ? '1' : '0');
        putchar(k + 1 == used ? '\n' : ' ');
    }
}

/*
 * bytes_for_values --
 *     Compute expected packed byte length for an array of values.
 */
static inline size_t
bytes_for_values(const uint64_t *vals, size_t n)
{
    size_t nibbles = 0;
    for (size_t i = 0; i < n; ++i)
        nibbles += __4b_nibbles_for_posint(vals[i]);
    return (nibbles + 1) >> 1; /* ceil(nibbles/2) */
}

/*
 * assert_bytes_for_values --
 *     Assert the packed length equals the expected byte length.
 */
static inline void
assert_bytes_for_values(size_t used, const uint64_t *vals, size_t n)
{
    size_t exp_used = bytes_for_values(vals, n);
    testutil_assert(used == exp_used);
}

/*
 * encode_array --
 *     Encode array.
 */
static void
encode_array(const uint64_t *vals, size_t nvals, uint8_t *buf, size_t bufsz, size_t *used_len)
{
    WT_4B_PACK_CONTEXT pctx;
    uint8_t *p = buf;
    size_t i;
    __4b_pack_init(&pctx, &p, buf + bufsz);
    for (i = 0; i < nvals; ++i)
        testutil_check(__4b_pack_posint_ctx(&pctx, vals[i]));
    *used_len = (size_t)(p - buf);
}

/*
 * decode_array --
 *     Decode array.
 */
static void
decode_array(const uint8_t *buf, size_t len, size_t count, uint64_t *out)
{
    WT_4B_UNPACK_CONTEXT uctx;
    const uint8_t *p = buf;
    size_t i;
    __4b_unpack_init(&uctx, &p, buf + len);
    for (i = 0; i < count; ++i)
        testutil_check(__4b_unpack_posint_ctx(&uctx, &out[i]));
}

/*
 * main --
 *     Main.
 */
int
main(void)
{
    uint8_t buf[1024];
    const size_t bufsz = sizeof(buf);

    /*
     * Required on some systems to pull in parts of the library for which we have data references.
     */
    testutil_check(__wt_library_init());

    /* Positive integers */
    printf("\n    Positive integers\n%-10s %-20s %-30s  %-20s\n", "Number", "Hex", "Bin",
      "Decoded Value");
    for (uint64_t i = 0; i <= 200; ++i) {
        uint64_t enc_vals[1] = {i};
        size_t used = 0;
        uint64_t dec = 0;
        encode_array(enc_vals, 1, buf, bufsz, &used);
        decode_array(buf, used, 1, &dec);
        /* Verify expected packed size in bytes: ceil(nibbles/2). */
        assert_bytes_for_values(used, enc_vals, 1);
        printf("%-10" PRIu64 " ", i);
        print_hex_bin_columns(buf, used);
        printf("  %-20" PRIu64 "\n", dec);
        testutil_assert(dec == i);
    }
    for (int ii = 3; ii < 30; ++ii) {
        uint64_t i = 200ULL + ((uint64_t)ii * ((uint64_t)1 << ii)) / 3ULL;
        uint64_t dec = 0;
        size_t used = 0;
        encode_array(&i, 1, buf, bufsz, &used);
        decode_array(buf, used, 1, &dec);
        /* Verify expected packed size. */
        assert_bytes_for_values(used, &i, 1);
        printf("%-10" PRIu64 " ", i);
        print_hex_bin_columns(buf, used);
        printf("  %-20" PRIu64 "\n", dec);
        testutil_assert(dec == i);
    }

    /* Signed integers via zigzag */
    printf(
      "\n    Signed integers\n%-10s %-20s %-20s %-20s\n", "Number", "Hex", "Bin", "Decoded Value");
    for (int64_t i = -100; i <= 100; ++i) {
        uint64_t enc = __wt_encode_signed_as_positive(i);
        uint64_t decpos = 0;
        int64_t dec = 0;
        size_t used = 0;
        encode_array(&enc, 1, buf, bufsz, &used);
        decode_array(buf, used, 1, &decpos);
        /* Verify expected packed size for the encoded positive. */
        assert_bytes_for_values(used, &enc, 1);
        dec = __wt_decode_positive_as_signed(decpos);
        printf("%-10" PRId64 " ", i);
        print_hex_bin_columns(buf, used);
        printf(" %-20" PRId64 "\n", dec);
        testutil_assert(dec == i);
    }

    /* Pairs of integers */
    printf("\n    Pairs of integers\n%-15s %-15s %-8s %-10s %-16s\n", "Array", "Decoded", "Len",
      "Hex", "Bin");
    printf(
      "\n    Pairs of integers\n"
      "Array\t"
      "Decoded\t"
      "Len\t"
      "Hex\t"
      "Bin\n");
    for (uint64_t i = 0; i <= 10; ++i) {
        uint64_t arr[2] = {i * (i + 1) / 2, i};
        uint64_t out[2] = {0, 0};
        size_t used = 0;
        encode_array(arr, 2, buf, bufsz, &used);
        decode_array(buf, used, 2, out);
        /* Verify expected packed size for two numbers. */
        assert_bytes_for_values(used, arr, 2);
        print_u64_array(arr, 2);
        printf(" ");
        print_u64_array(out, 2);
        printf(" %" WT_SIZET_FMT "\t", used);
        print_hex_bin_columns(buf, used);
        printf("\n");
        testutil_assert(out[0] == arr[0] && out[1] == arr[1]);
    }

    /* Array of small positive integers */
    printf("\n    Array of small integers\n");
    for (uint64_t i = 1; i <= 10; ++i) {
        uint64_t arr[10], out[10];
        size_t used = 0;
        for (uint64_t j = 0; j < i; ++j)
            arr[j] = j;
        encode_array(arr, i, buf, bufsz, &used);
        decode_array(buf, used, i, out);
        /* Verify expected packed size for array. */
        assert_bytes_for_values(used, arr, (size_t)i);
        printf("Array:    ");
        print_u64_array(arr, (size_t)i);
        printf("\t(%" PRIu64 " elements)\n", i);
        printf("Decoded:  ");
        print_u64_array(out, (size_t)i);
        printf("\n");
        for (uint64_t j = 0; j < i; ++j)
            testutil_assert(out[j] == arr[j]);
        print_hex_dump(buf, used);
        print_bin_dump(buf, used);
    }

    /* Array of bigger integers (squares) */
    printf("\n    Array of bigger integers\n");
    for (uint64_t i = 2; i <= 10; ++i) {
        uint64_t arr[10], out[10];
        size_t used = 0;
        for (uint64_t j = 0; j < i; ++j)
            arr[j] = j * j;
        encode_array(arr, i, buf, bufsz, &used);
        decode_array(buf, used, i, out);
        /* Verify expected packed size for array. */
        assert_bytes_for_values(used, arr, (size_t)i);
        printf("Array:    ");
        print_u64_array(arr, (size_t)i);
        printf("\t(%" PRIu64 " elements)\n", i);
        printf("Decoded:  ");
        print_u64_array(out, (size_t)i);
        printf("\n");
        for (uint64_t j = 0; j < i; ++j)
            testutil_assert(out[j] == arr[j]);
        print_hex_dump(buf, used);
        print_bin_dump(buf, used);
    }

    return (0);
}
