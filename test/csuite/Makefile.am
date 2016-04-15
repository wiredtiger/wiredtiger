AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include \
    -I$(top_srcdir)/test/utility
LDADD = $(top_builddir)/libwiredtiger.la \
    $(top_builddir)/test/utility/libtest_util.la
AM_LDFLAGS = -static

test_wt1965_SOURCES = test_wt1965.c
noinst_PROGRAMS = test_wt1965

test_wt2246_SOURCES = test_wt2246.c
noinst_PROGRAMS += test_wt2246

test_wt2535_SOURCES = test_wt2535.c
noinst_PROGRAMS += test_wt2535

# Run this during a "make check" smoke test.
TESTS = $(noinst_PROGRAMS)
LOG_COMPILER = $(TEST_WRAPPER)

clean-local:
	rm -rf WT_TEST.* *.core
