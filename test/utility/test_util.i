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
#include "wt_internal.h"			/* For __wt_XXX */

#ifdef _WIN32
#include "windows_shim.h"
#endif

#ifdef _WIN32
	#define DIR_DELIM '\\'
	#define RM_COMMAND "rd /s /q "
#else
	#define	DIR_DELIM '/'
	#define RM_COMMAND "rm -rf "
#endif

#define	DEFAULT_DIR "WT_TEST"
#define	MKDIR_COMMAND "mkdir "

/* Allow tests to add their own death handling. */
extern void (*custom_die)(void);

static void	 testutil_die(int, const char *, ...)
#if defined(__GNUC__)
__attribute__((__noreturn__))
#endif
;

/*
 * die --
 *	Report an error and quit.
 */
static void
testutil_die(int e, const char *fmt, ...)
{
	va_list ap;

	/* Allow test programs to cleanup on fatal error. */
	if (custom_die != NULL)
		(*custom_die)();

	if (fmt != NULL) {
		va_start(ap, fmt);
		vfprintf(stderr, fmt, ap);
		va_end(ap);
	}
	if (e != 0)
		fprintf(stderr, ": %s", wiredtiger_strerror(e));
	fprintf(stderr, "\n");

	exit(EXIT_FAILURE);
}

/*
 * testutil_check --
 *	Complain and quit if a function call fails.
 */
#define	testutil_check(call) do {					\
	int __r;							\
	if ((__r = (call)) != 0)					\
		testutil_die(__r, "%s/%d: %s", __func__, __LINE__, #call);\
} while (0)

/*
 * testutil_checkfmt --
 *	Complain and quit if a function call fails, with additional arguments.
 */
#define	testutil_checkfmt(call, fmt, ...) do {				\
	int __r;							\
	if ((__r = (call)) != 0)					\
		testutil_die(__r, "%s/%d: %s: " fmt,			\
		    __func__, __LINE__, #call, __VA_ARGS__);		\
} while (0)

/*
 * testutil_work_dir_from_path --
 *	Takes a buffer, its size and the intended work directory.
 *	Creates the full intended work directory in buffer.
 */
static inline void
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
static inline void
testutil_clean_work_dir(char *dir)
{
	size_t len;
	int ret;
	char *buf;

	/* Additional bytes for the Windows rd command. */
	len = strlen(dir) + strlen(RM_COMMAND) + 1;
	if ((buf = malloc(len)) == NULL)
		testutil_die(ENOMEM, "Failed to allocate memory");

	snprintf(buf, len, "%s%s", RM_COMMAND, dir);

	if ((ret = system(buf)) != 0 && ret != ENOENT)
		testutil_die(ret, "%s", buf);
	free(buf);
}

/*
 * testutil_make_work_dir --
 *	Delete the existing work directory, then create a new one.
 */
static inline void
testutil_make_work_dir(char *dir)
{
	size_t len;
	int ret;
	char *buf;

	testutil_clean_work_dir(dir);

	/* Additional bytes for the mkdir command */
	len = strlen(dir) + strlen(MKDIR_COMMAND) + 1;
	if ((buf = malloc(len)) == NULL)
		testutil_die(ENOMEM, "Failed to allocate memory");

	/* mkdir shares syntax between Windows and Linux */
	snprintf(buf, len, "%s%s", MKDIR_COMMAND, dir);
	if ((ret = system(buf)) != 0)
		testutil_die(ret, "%s", buf);
	free(buf);
}

/*
 * dcalloc --
 *	Call calloc, dying on failure.
 */
static inline void *
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
static inline void *
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
static inline void *
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
static inline void *
dstrdup(const void *str)
{
	char *p;

	if ((p = strdup(str)) != NULL)
		return (p);
	testutil_die(errno, "strdup");
}
