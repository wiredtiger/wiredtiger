#include "test_util.h"

#include <signal.h>
#include <sys/wait.h>

#define	NUM_KEYS	2000

static int
las_workload(TEST_OPTS *opts, const char *las_file_max)
{
	WT_CURSOR *cursor;
	WT_SESSION *other_session, *session;
	int i;
	char buf[WT_MEGABYTE], open_config[128];

	WT_RET(__wt_snprintf(open_config, sizeof(open_config),
	    "create,cache_size=50MB,cache_overflow=(file_max=%s)",
	    las_file_max));

	WT_RET(wiredtiger_open(opts->home, NULL, open_config, &opts->conn));
	WT_RET(opts->conn->open_session(opts->conn, NULL, NULL, &session));
	WT_RET(
	    session->create(session, opts->uri, "key_format=i,value_format=S"));
	WT_RET(session->open_cursor(session, opts->uri, NULL, NULL, &cursor));

	memset(buf, 0xA, WT_MEGABYTE);
	buf[WT_MEGABYTE - 1] = '\0';

	/* Populate the table. */
	for (i = 0; i < NUM_KEYS; ++i) {
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, buf);
		WT_RET(cursor->insert(cursor));
	}

	/*
	 * Open a snapshot isolation transaction in another session. This forces
	 * the cache to retain all previous values. Then update all keys with a
	 * new value in the original session while keeping that snapshot
	 * transaction open. With the large value buffer, small cache and lots
	 * of keys, this will force a lot of lookaside usage.
	 *
	 * When the file_max setting is small, the maximum size should easily be
	 * reached and we should panic. When the maximum size is large or not
	 * set, then we should succeed.
	 */
	WT_RET(
	    opts->conn->open_session(opts->conn, NULL, NULL, &other_session));
	other_session->begin_transaction(other_session, "isolation=snapshot");

	memset(buf, 0xB, WT_MEGABYTE);
	buf[WT_MEGABYTE - 1] = '\0';

	for (i = 0; i < NUM_KEYS; ++i) {
		cursor->set_key(cursor, i);
		cursor->set_value(cursor, buf);
		WT_RET(cursor->update(cursor));
	}

	/*
	 * Cleanup.
	 * We do not get here when the file_max size is small because we will
	 * have already hit the maximum and exited. This code only executes on
	 * the successful path.
	 */
	WT_RET(other_session->rollback_transaction(other_session, NULL));
	WT_RET(other_session->close(other_session, NULL));

	WT_RET(cursor->close(cursor));
	WT_RET(session->close(session, NULL));

	return (0);
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

	/*
	 * Since it's possible that the workload will panic and abort, we will
	 * fork the process and execute the workload in the child process.
	 *
	 * This way, we can safely check the exit code of the child process and
	 * confirm that it is what we expected.
	 */
	pid = fork();
	if (pid < 0)
		/* Failed fork. */
		testutil_die(errno, "fork");
	else if (pid == 0) {
		/* Child process from here. */
		status = las_workload(&opts, las_file_max);
		exit(status);
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

	ret = test_las_workload(argc, argv, "0");
	testutil_assert(ret == 0);

	ret = test_las_workload(argc, argv, "5GB");
	testutil_assert(ret == 0);

	ret = test_las_workload(argc, argv, "100MB");
	testutil_assert(
	    ret != 0 && WIFSIGNALED(ret) && WTERMSIG(ret) == SIGABRT);

	return (0);
}
