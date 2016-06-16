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
 * JIRA ticket reference: WT-2611
 * Test case description:
 * Failure mode:
 */

void (*custom_die)(void) = NULL;

int
main(int argc, char *argv[])
{
	WT_CONFIG_ITEM k,v;
	WT_CONFIG_PARSER *parser, *parser2, *parser3;
	TEST_OPTS *opts, _opts;
	const char *config_string, *quoted;
	int ret;
	bool found;

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	/* Here we use a config string with quoting */
	config_string =
	    "path=/dev/loop,page_size=1024,log=(archive=true,file_max=20MB),"
	    "statistics_log=(sources=(\"table:a\"),wait=10)";
	testutil_check(wiredtiger_config_parser_open(
	    NULL, config_string, strlen(config_string), &parser));
	testutil_check(parser->get(parser, "statistics_log", &v));
	printf("statistics_log=\"%s\"\n", v.str);
	testutil_assert(strncmp(v.str, "(sources=(\"table:a\"),wait=10)",
	    v.len) == 0);
	testutil_check(wiredtiger_config_parser_open(
	    NULL, v.str, v.len, &parser2));
	testutil_check(parser2->get(parser2, "sources", &v));
	printf("sources=\"%s\"\n", v.str);
	testutil_assert(strncmp(v.str, "(\"table:a\")", v.len) == 0);
	testutil_check(wiredtiger_config_parser_open(
	    NULL, v.str, v.len, &parser3));
	while ((ret = parser3->next(parser3, &k, &v)) == 0) {
		printf("%.*s:", (int)k.len, k.str);
		if (v.type == WT_CONFIG_ITEM_NUM)
			printf("%d\n", (int)v.val);
		else
			printf("%.*s\n", (int)v.len, v.str);
	}
	testutil_assert(ret = WT_NOTFOUND);
	testutil_check(parser3->close(parser3));
	testutil_check(parser2->close(parser2));
	testutil_check(parser->close(parser));

	/*
	 * Here we make sure we can fully quote our original string,
	 * and get it back.
	 */
	quoted =
	    "quoted=\"path=/dev/loop,page_size=1024,"
	    "log=(archive=true,file_max=20MB),"
	    "statistics_log=(sources=(\\\"table:a\\\"),wait=10)\"";
	testutil_check(wiredtiger_config_parser_open(
	    NULL, quoted, strlen(quoted), &parser));
	testutil_check(parser->get(parser, "quoted", &v));
	testutil_assert(strncmp(v.str, config_string, v.len) == 0);
	testutil_check(parser->close(parser));

	testutil_check(wiredtiger_config_parser_open(
	    NULL, quoted, strlen(quoted), &parser));
	found = false;
	while ((ret = parser->next(parser, &k, &v)) == 0) {
		printf("%.*s:", (int)k.len, k.str);
		if (v.type == WT_CONFIG_ITEM_STRING && k.len == 6 &&
		    strncmp(k.str, "quoted", 6) == 0) {
			testutil_assert(
			    strncmp(v.str, config_string, v.len) == 0);
			found = true;
		}
	}
	testutil_assert(ret = WT_NOTFOUND);
	testutil_assert(found);
	testutil_check(parser->close(parser));

	testutil_cleanup(opts);

	return (0);
}
