/*-
 * Public Domain 2014-2016 MongoDB, Inc.
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
 * testutil_parse_opts --
 *    Parse command line options for a test case.
 */
int
testutil_parse_opts(int argc, char *argv[], TEST_OPTS *opts)
{
	int ch;
	size_t len;

	opts->preserve = false;
	opts->running = true;
	opts->verbose = false;

	if ((opts->progname = strrchr(argv[0], '/')) == NULL)
		opts->progname = argv[0];
	else
		++opts->progname;

	while ((ch = getopt(argc, argv, "h:n:o:pvA:R:T:W:")) != EOF)
		switch (ch) {
		case 'h': /* Home directory */
			opts->home = optarg;
			break;
		case 'n': /* Number of records */
			opts->nrecords = (uint64_t)atoll(optarg);
			break;
		case 'o': /* Number of operations */
			opts->nops = (uint64_t)atoll(optarg);
			break;
		case 'p': /* Preserve directory contents */
			opts->preserve = true;
			break;
		case 't': /* Table type */
			switch (optarg[0]) {
			case 'c':
			case 'C':
				opts->table_type = TABLE_COL;
				break;
			case 'f':
			case 'F':
				opts->table_type = TABLE_FIX;
				break;
			case 'r':
			case 'R':
				opts->table_type = TABLE_ROW;
				break;
			}
			break;
		case 'v': /* Number of append threads */
			opts->verbose = true;
			break;
		case 'A': /* Number of append threads */
			opts->n_append_threads = (uint64_t)atoll(optarg);
			break;
		case 'R': /* Number of reader threads */
			opts->n_read_threads = (uint64_t)atoll(optarg);
			break;
		case 'T': /* Number of threads */
			opts->nthreads = (uint64_t)atoll(optarg);
			break;
		case 'W': /* Number of writer threads */
			opts->n_write_threads = (uint64_t)atoll(optarg);
			break;
		case '?':
		default:
			(void)fprintf(stderr, "usage: %s "
			    "[-h home] "
			    "[-n record count] "
			    "[-o op count] "
			    "[-t table type] "
			    "[-A append thread count] "
			    "[-R read thread count] "
			    "[-T thread count] "
			    "[-W write thread count] ",
			    opts->progname);
			return (1);
		}

	/*
	 * Setup the home directory. It needs to be unique for every test
	 * or the auto make parallel tester gets upset.
	 */
	len = snprintf(NULL, 0, "WT_TEST.%s", opts->progname) + 1;
	opts->home = (char *)malloc(len);
	snprintf(opts->home, len, "WT_TEST.%s", opts->progname);

	/* Setup the default URI string */
	len = snprintf(NULL, 0, "table:%s", opts->progname) + 1;
	opts->uri = (char *)malloc(len);
	snprintf(opts->uri, len, "table:%s", opts->progname);

	return (0);
}
