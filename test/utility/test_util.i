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

/* Generic option parsing structure shared by all test cases. */
typedef struct {
	char  *home;
	char  *progname;
	enum {	TABLE_COL=1,	/* Fixed-length column store */
		TABLE_FIX=2,	/* Variable-length column store */
		TABLE_ROW=3	/* Row-store */
	} table_type;
	bool	 preserve;
	bool	 verbose;
	uint64_t     nrecords;
	uint64_t     nops;
	uint64_t     nthreads;
	uint64_t     n_append_threads;
	uint64_t     n_read_threads;
	uint64_t     n_write_threads;

	/*
	 * Fields commonly shared within a test program. The test cleanup
	 * function will attempt to automatically free and close non-null
	 * resources.
	 */
	WT_CONNECTION *conn;
	char	  *conn_config;
	WT_SESSION    *session;
	bool	   running;
	char	  *table_config;
	char	  *uri;
	volatile uint64_t   next_threadid;
	uint64_t   max_inserted_id;
} TEST_OPTS;

/*
 * testutil_parse_opts --
 *    Parse command line options for a test case.
 */
static inline int
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
 * testutil_cleanup --
 *	Delete the existing work directory and free the options structure.
 */
static inline void
testutil_cleanup(TEST_OPTS *opts)
{

	if (opts->conn != NULL)
		testutil_check(opts->conn->close(opts->conn, NULL));

	if (!opts->preserve)
		testutil_clean_work_dir(opts->home);

	if (opts->conn_config != NULL)
		free(opts->conn_config);
	if (opts->table_config != NULL)
		free(opts->table_config);
	if (opts->uri != NULL)
		free(opts->uri);
	free(opts->home);
}
