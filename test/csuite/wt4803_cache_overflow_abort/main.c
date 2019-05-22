#include "test_util.h"

#include <signal.h>
#include <sys/wait.h>

#define	CONFIG_SIZE	100

static void
las_workload(TEST_OPTS *opts, const char *las_file_max)
{
	WT_CURSOR *cursor;
	WT_SESSION *other_session, *session;
	int i;
	char buf[WT_MEGABYTE], open_config[CONFIG_SIZE];

	testutil_check(__wt_snprintf(open_config, CONFIG_SIZE,
	    "create,cache_size=50MB,cache_overflow=(file_max=%s)",
	    las_file_max));

	testutil_check(
	    wiredtiger_open(opts->home, NULL, open_config, &opts->conn));
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &session));
	testutil_check(
	    session->create(session, opts->uri, "key_format=i,value_format=S"));
	testutil_check(
	    session->open_cursor(session, opts->uri, NULL, NULL, &cursor));

	/* Populate the table. */
	for (i = 0; i < 2000; ++i) {
		memset(buf, 0xA, WT_MEGABYTE);
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, buf);
		testutil_check(cursor->insert(cursor));
	}

	/* Begin another transaction. */
	testutil_check(
	    opts->conn->open_session(opts->conn, NULL, NULL, &other_session));
	other_session->begin_transaction(other_session, "isolation=snapshot");

	/*
	 * And at the same time, do a bunch of updates. Since we've got a
	 * transaction running with snapshot isolation, we're going to have to
	 * retain the previous values which will give lookaside a run for its
	 * money.
	 *
	 * Since the small file_max example is only 100MB, it shouldn't take
	 * much. I'm doing 2000 1MB updates for good measure since we
	 * automatically use snappy for lookaside.
	 */
	for (i = 0; i < 2000; ++i) {
		memset(buf, 0xB, WT_MEGABYTE);
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, buf);
		testutil_check(cursor->update(cursor));
	}

	/* Cleanup. */
	testutil_check(
	    other_session->rollback_transaction(other_session, NULL));
	testutil_check(other_session->close(other_session, NULL));

	testutil_check(cursor->close(cursor));
	testutil_check(session->close(session, NULL));
}

static int
test_las_workload(int argc, char **argv, const char *las_file_max)
{
	TEST_OPTS opts;
	pid_t pid;
	int status;

	memset(&opts, 0x0, sizeof(opts));
	testutil_check(testutil_parse_opts(argc, argv, &opts));
	testutil_make_work_dir(opts.home);

	pid = fork();
	if (pid < 0)
		/* Failed fork. */
		testutil_die(errno, "fork");
	else if (pid == 0) {
		/* Child process from here. */
		las_workload(&opts, las_file_max);
		exit(EXIT_SUCCESS);
		return (EXIT_SUCCESS);
	}

	/* Parent process from here. */
	if (waitpid(pid, &status, 0) == -1)
		testutil_die(errno, "waitpid");

	testutil_cleanup(&opts);
	return (status);
}

int
main(int argc, char **argv)
{
	int ret;

	ret = test_las_workload(argc, argv, "1GB");
	testutil_assert(ret == 0);

	ret = test_las_workload(argc, argv, "100MB");
	testutil_assert(ret != 0);

	return (0);
}
