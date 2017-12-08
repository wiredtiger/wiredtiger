/*-
 * Public Domain 2014-2017 MongoDB, Inc.
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

void
show_bits(WT_BITMAP *bm, const char *msg)
{
	uint64_t bit, endbit;
	uint8_t byte;
	uint8_t *p;

	printf("%s bitmap[%" PRIu64 "/%" PRIu64"] = {", msg, bm->cnt, bm->size);
	p = bm->bitstring;
	for (bit = 0; bit < bm->size; ) {
		endbit = bit + 8;
		byte = *p++;
		if (bm->size < endbit)
			endbit = bm->size;
		if (byte == 0)
			bit = endbit;
		else
			for (; bit < endbit; bit++) {
				if ((byte & 0x1) != 0) {
					printf(" %" PRIu64, bit);
					if (bit > bm->cnt)
						printf("(***OUT OF RANGE***)");
				}
				byte >>= 1;
			}
	}
	printf(" }\n");
}

WT_BITMAP a, b, c;
uint64_t maxidx_values[] = { 1, 2, 3, 10, 100, 1000 };

void
run()
{
	uint64_t bit;
	uint64_t i, max, maxidx;
	bool set;

	/* Tests for all __wt_bitmap_* functions. */
	for (maxidx = 0;
	     maxidx < sizeof(maxidx_values) / sizeof(maxidx_values[0]);
	     maxidx++) {
		memset(&a, 0, sizeof(a));
		memset(&b, 0, sizeof(a));
		memset(&c, 0, sizeof(a));
		max = maxidx_values[maxidx];
		printf("RUN with max = %" PRIu64 "\n", max);

		show_bits(&a, "empty");
		for (i = 0; i < max; i++) {
			testutil_check(__wt_bitmap_alloc_bit(NULL, &a, &bit));
			testutil_assert(bit == i);
		}
		__wt_bitmap_clear_all(&a);
		show_bits(&a, "cleared");
		__wt_bitmap_copy_bitmap(NULL, &c, &a);
		show_bits(&c, "copied");
		for (i = 0; i < max; i += 5)
			__wt_bitmap_set(NULL, &a, i);
		show_bits(&a, "5");
		__wt_bitmap_copy_bitmap(NULL, &b, &a);
		__wt_bitmap_clear_all(&b);
		for (i = 0; i < max; i += 3)
			__wt_bitmap_set(NULL, &b, i);
		show_bits(&b, "3");
		__wt_bitmap_or_bitmap(NULL, &c, &b);
		show_bits(&c, "3 from or");
		__wt_bitmap_or_bitmap(NULL, &c, &a);
		show_bits(&c, "3,5 from or");

		/* c has bits on that are divisible by 5 and 3. */
		if (max >= 2) {
			testutil_check(__wt_bitmap_alloc_bit(NULL, &c, &bit));
			testutil_assert(bit == 1);
		}
		if (max >= 3) {
			testutil_check(__wt_bitmap_alloc_bit(NULL, &c, &bit));
			testutil_assert(bit == 2);
		}
		if (max >= 5) {
			testutil_check(__wt_bitmap_alloc_bit(NULL, &c, &bit));
			testutil_assert(bit == 4);
		}
		if (max >= 8) {
			testutil_check(__wt_bitmap_alloc_bit(NULL, &c, &bit));
			testutil_assert(bit == 7);
		}
		for (i = 0; i < max; i++) {
			set = (i % 5 == 0 || i % 3 == 0 || i < 8);
			testutil_assert(set == __wt_bitmap_test(NULL, &c, i));
		}
		show_bits(&c, "3,5, values less than 7");
		if (max >= 100) {
			__wt_bitmap_clear_all(&a);
			__wt_bitmap_clear_all(&b);
			testutil_assert(!__wt_bitmap_test_any(&a));
			__wt_bitmap_set(NULL, &a, 11);
			testutil_assert(__wt_bitmap_test_any(&a));
			__wt_bitmap_set(NULL, &b, 12);
			testutil_assert(!__wt_bitmap_test_bitmap(NULL, &a, &b));
			__wt_bitmap_set(NULL, &b, 11);
			testutil_assert(__wt_bitmap_test_bitmap(NULL, &a, &b));
			__wt_bitmap_set(NULL, &a, 13);
			__wt_bitmap_clear_bitmap(NULL, &a, &b);
			testutil_assert(__wt_bitmap_test_any(&a));
			__wt_bitmap_set(NULL, &b, 13);
			__wt_bitmap_clear_bitmap(NULL, &a, &b);
			testutil_assert(!__wt_bitmap_test_any(&a));
			__wt_bitmap_set(NULL, &a, 17);
			testutil_assert(__wt_bitmap_test_any(&a));
			__wt_bitmap_clear(NULL, &a, 17);
			testutil_assert(!__wt_bitmap_test_any(&a));

			__wt_bitmap_copy_bitmap(NULL, &a, &c);
			__wt_bitmap_clear_all(&b);
			__wt_bitmap_set(NULL, &b, 11);
			__wt_bitmap_set(NULL, &b, 20);
			__wt_bitmap_or_bitmap(NULL, &a, &b);
			for (i = 0; i < max; i++) {
				set = (i % 5 == 0 || i % 3 == 0 || i < 8 ||
				    i == 11);
				testutil_assert(set == __wt_bitmap_test(
				    NULL, &a, i));
			}
			__wt_bitmap_set(NULL, &a, 1001);
			testutil_assert(!__wt_bitmap_test(NULL, &a, 1000));
			testutil_assert(__wt_bitmap_test(NULL, &a, 1001));
		}
		__wt_bitmap_free(NULL, &a);
		__wt_bitmap_free(NULL, &b);
		__wt_bitmap_free(NULL, &c);
		printf("\n");
	}
}

int
main(int argc, char *argv[])
{
	(void)testutil_set_progname(argv);
	run();

	return (EXIT_SUCCESS);
}
