#include "test_util.h"

int
main(int argc, char *argv[])
{
	TEST_OPTS *opts, _opts;
	WT_SESSION *session;
	char posturi[256];
	char baluri[256];
	char flaguri[256];
	const char *tablename;

	opts = &_opts;
	memset(opts, 0, sizeof(*opts));
	testutil_check(testutil_parse_opts(argc, argv, opts));
	testutil_make_work_dir(opts->home);

	tablename = strchr(opts->uri, ':');
	testutil_assert(tablename != NULL);
	tablename++;
	testutil_check(__wt_snprintf(posturi, sizeof(posturi),
	    "index:%s:post", tablename));
	testutil_check(__wt_snprintf(baluri, sizeof(baluri),
	    "index:%s:bal", tablename));
	testutil_check(__wt_snprintf(flaguri, sizeof(flaguri),
	    "index:%s:flag", tablename));

	testutil_check(wiredtiger_open(
	    opts->home, NULL, "create,cache_size=100M", &opts->conn));

	testutil_check(opts->conn->open_session(
	    opts->conn, NULL, NULL, &session));

	testutil_check(session->create(
	    session, opts->uri, "key_format=i,value_format=iii,"
	    "columns=(id,post,bal,flag)"));

	testutil_check(session->create(session, posturi, "columns=(post)"));
	testutil_check(session->create(session, baluri, "columns=(bal)"));
	testutil_check(session->create(session, flaguri, "columns=(flag)"));

	testutil_check(session->drop(session, posturi, NULL));
	testutil_check(session->drop(session, baluri, NULL));
	testutil_check(session->drop(session, flaguri, NULL));
	testutil_check(session->drop(session, opts->uri, NULL));
	testutil_check(session->close(session, NULL));
	testutil_check(opts->conn->close(opts->conn, NULL));
	opts->conn = NULL;

	testutil_cleanup(opts);

	return (0);
}
