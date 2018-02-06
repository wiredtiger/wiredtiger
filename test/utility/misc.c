/*-
 * Public Domain 2014-2018 MongoDB, Inc.
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

void (*custom_die)(void) = NULL;
const char *progname = "program name not set";

/*
 * testutil_die --
 *	Report an error and abort.
 */
void
testutil_die(int e, const char *fmt, ...)
{
	va_list ap;

	/* Flush output to be sure it doesn't mix with fatal errors. */
	(void)fflush(stdout);
	(void)fflush(stderr);

	/* Allow test programs to cleanup on fatal error. */
	if (custom_die != NULL)
		(*custom_die)();

	fprintf(stderr, "%s: FAILED", progname);
	if (fmt != NULL) {
		fprintf(stderr, ": ");
		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);
	}
	if (e != 0)
		fprintf(stderr, ": %s", wiredtiger_strerror(e));
	fprintf(stderr, "\n");
	fprintf(stderr, "process aborting\n");

	abort();
}

/*
 * testutil_set_progname --
 *	Set the global program name for error handling.
 */
const char *
testutil_set_progname(char * const *argv)
{
	if ((progname = strrchr(argv[0], DIR_DELIM)) == NULL)
		progname = argv[0];
	else
		++progname;
	return (progname);
}

/*
 * testutil_work_dir_from_path --
 *	Takes a buffer, its size and the intended work directory.
 *	Creates the full intended work directory in buffer.
 */
void
testutil_work_dir_from_path(char *buffer, size_t len, const char *dir)
{
	/* If no directory is provided, use the default. */
	if (dir == NULL)
		dir = DEFAULT_DIR;

	if (len < strlen(dir) + 1)
		testutil_die(ENOMEM,
		    "Not enough memory in buffer for directory %s", dir);

	strcpy(buffer, dir);
}

/*
 * testutil_clean_work_dir --
 *	Remove the work directory.
 */
void
testutil_clean_work_dir(const char *dir)
{
	size_t len;
	int ret;
	char *buf;

#ifdef _WIN32
	/* Additional bytes for the Windows rd command. */
	len = 2 * strlen(dir) + strlen(RM_COMMAND) +
		strlen(DIR_EXISTS_COMMAND) + 4;
	if ((buf = malloc(len)) == NULL)
		testutil_die(ENOMEM, "Failed to allocate memory");

	testutil_check(__wt_snprintf(
	    buf, len, "%s %s %s %s", DIR_EXISTS_COMMAND, dir, RM_COMMAND, dir));
#else
	len = strlen(dir) + strlen(RM_COMMAND) + 1;
	if ((buf = malloc(len)) == NULL)
		testutil_die(ENOMEM, "Failed to allocate memory");

	testutil_check(__wt_snprintf(buf, len, "%s%s", RM_COMMAND, dir));
#endif

	if ((ret = system(buf)) != 0 && ret != ENOENT)
		testutil_die(ret, "%s", buf);
	free(buf);
}

/*
 * testutil_make_work_dir --
 *	Delete the existing work directory, then create a new one.
 */
void
testutil_make_work_dir(const char *dir)
{
	size_t len;
	char *buf;

	testutil_clean_work_dir(dir);

	/* Additional bytes for the mkdir command */
	len = strlen(dir) + strlen(MKDIR_COMMAND) + 1;
	if ((buf = malloc(len)) == NULL)
		testutil_die(ENOMEM, "Failed to allocate memory");

	/* mkdir shares syntax between Windows and Linux */
	testutil_check(__wt_snprintf(buf, len, "%s%s", MKDIR_COMMAND, dir));
	testutil_check(system(buf));
	free(buf);
}

/*
 * testutil_progress --
 *	Print a progress message to the progress file.
 */
void
testutil_progress(TEST_OPTS *opts, const char *message)
{
	FILE *fp;
	time_t now;

	if ((fp = fopen(opts->progress_file_name, "a")) == NULL)
		testutil_die(errno, "fopen");
	(void)time(&now);
	fprintf(fp, "[%" PRIuMAX "] %s\n", (uintmax_t)now, message);
	if (fclose(fp) != 0)
		testutil_die(errno, "fclose");
}

/*
 * testutil_cleanup --
 *	Delete the existing work directory and free the options structure.
 */
void
testutil_cleanup(TEST_OPTS *opts)
{
	if (opts->conn != NULL)
		testutil_check(opts->conn->close(opts->conn, NULL));

	if (!opts->preserve)
		testutil_clean_work_dir(opts->home);

	free(opts->uri);
	free(opts->progress_file_name);
	free(opts->home);
}

/*
 * testutil_is_flag_set --
 *	Return if an environment variable flag is set.
 */
bool
testutil_is_flag_set(const char *flag)
{
	const char *res;
	bool enable_long_tests;

	if (__wt_getenv(NULL, flag, &res) == WT_NOTFOUND)
		return (false);

	/*
	 * This is a boolean test. So if the environment variable is set to any
	 * value other than 0, we return success.
	 */
	enable_long_tests = res[0] != '0';

	free((void *)res);

	return (enable_long_tests);
}

/*
 * dcalloc --
 *	Call calloc, dying on failure.
 */
void *
dcalloc(size_t number, size_t size)
{
	void *p;

	if ((p = calloc(number, size)) != NULL)
		return (p);
	testutil_die(errno, "calloc: %" WT_SIZET_FMT "B", number * size);
}

/*
 * dmalloc --
 *	Call malloc, dying on failure.
 */
void *
dmalloc(size_t len)
{
	void *p;

	if ((p = malloc(len)) != NULL)
		return (p);
	testutil_die(errno, "malloc: %" WT_SIZET_FMT "B", len);
}

/*
 * drealloc --
 *	Call realloc, dying on failure.
 */
void *
drealloc(void *p, size_t len)
{
	void *t;

	if ((t = realloc(p, len)) != NULL)
		return (t);
	testutil_die(errno, "realloc: %" WT_SIZET_FMT "B", len);
}

/*
 * dstrdup --
 *	Call strdup, dying on failure.
 */
void *
dstrdup(const void *str)
{
	char *p;

	if ((p = strdup(str)) != NULL)
		return (p);
	testutil_die(errno, "strdup");
}

/*
 * dstrndup --
 *      Call emulating strndup, dying on failure. Don't use actual strndup here
 *	as it is not supported within MSVC.
 */
void *
dstrndup(const char *str, size_t len)
{
	char *p;

	p = dcalloc(len + 1, sizeof(char));
	memcpy(p, str, len);
	return (p);
}

/*
 * example_setup --
 *	Set the program name, create a home directory for the example programs.
 */
const char *
example_setup(int argc, char * const *argv)
{
	const char *home;

	(void)argc;					/* Unused variable */

	(void)testutil_set_progname(argv);

	/*
	 * Create a clean test directory for this run of the test program if the
	 * environment variable isn't already set (as is done by make check).
	 */
	if ((home = getenv("WIREDTIGER_HOME")) == NULL)
		home = "WT_HOME";
	testutil_make_work_dir(home);
	return (home);
}
