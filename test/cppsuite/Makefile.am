AM_CPPFLAGS = -std=c++17 -I$(top_builddir) -I$(top_srcdir)/src/include \
    -I$(top_srcdir)/test/utility
LDADD = $(top_builddir)/test/utility/libtest_util.la \
    $(top_builddir)/libwiredtiger.la -lstdc++fs
AM_LDFLAGS = -static

all_TESTS=
noinst_PROGRAMS=

test_poc_SOURCES = tests/poc.cxx
noinst_PROGRAMS += test_poc
all_TESTS += test_poc

# Run this during a "make check" smoke test.
TESTS = $(all_TESTS)

clean-local:
	rm -rf WT_TEST.* core.* *.core
