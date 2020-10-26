AM_CPPFLAGS = -fsanitize=fuzzer,address
AM_CPPFLAGS += -I$(top_builddir)
AM_CPPFLAGS += -I$(top_srcdir)/src/include
AM_CPPFLAGS += -I$(top_srcdir)/test/utility

AM_LDFLAGS = -fsanitize=fuzzer,address -static

noinst_PROGRAMS = test_fuzz
test_fuzz_SOURCES = test_fuzz.c

test_fuzz_LDADD = $(top_builddir)/libwiredtiger.la
test_fuzz_LDADD += $(top_builddir)/test/utility/libtest_util.la
