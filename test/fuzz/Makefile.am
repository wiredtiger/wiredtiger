AM_CPPFLAGS = -fsanitize=fuzzer
AM_CPPFLAGS += -I$(top_builddir)
AM_CPPFLAGS += -I$(top_srcdir)/src/include
AM_CPPFLAGS += -I$(top_srcdir)/test/utility
AM_CPPFLAGS += -I$(top_srcdir)/test/fuzz

AM_LDFLAGS = -fsanitize=fuzzer -static

noinst_PROGRAMS = fuzz_config fuzz_modify

fuzz_config_SOURCES = config/fuzz_config.c fuzz_util.c
fuzz_config_LDADD = $(top_builddir)/libwiredtiger.la
fuzz_config_LDADD += $(top_builddir)/test/utility/libtest_util.la

fuzz_modify_SOURCES = modify/fuzz_modify.c fuzz_util.c
fuzz_modify_LDADD = $(top_builddir)/libwiredtiger.la
fuzz_modify_LDADD += $(top_builddir)/test/utility/libtest_util.la
