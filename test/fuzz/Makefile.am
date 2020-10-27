AM_CPPFLAGS = -fsanitize=fuzzer,address
AM_CPPFLAGS += -I$(top_builddir)
AM_CPPFLAGS += -I$(top_srcdir)/src/include
AM_CPPFLAGS += -I$(top_srcdir)/test/utility
AM_CPPFLAGS += -I$(top_srcdir)/test/fuzz

AM_LDFLAGS = -fsanitize=fuzzer,address -static

noinst_PROGRAMS = fuzz_config
fuzz_config_SOURCES = config/fuzz_config.c fuzz_util.c

fuzz_config_LDADD = $(top_builddir)/libwiredtiger.la
fuzz_config_LDADD += $(top_builddir)/test/utility/libtest_util.la
