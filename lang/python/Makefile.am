PYTHON = python3
PYSRC = $(top_srcdir)/lang/python
PYDIRS = -t $(abs_builddir) -I $(abs_top_srcdir):$(abs_top_builddir) -L $(abs_top_builddir)/.libs
PYTARGET = $(abs_builddir)/wiredtiger
all-local: _wiredtiger.so

# We keep generated Python sources under lang/python: that's where they live
# in release packages.
$(PYSRC)/wiredtiger_wrap.c: $(top_srcdir)/src/include/wiredtiger.in $(PYSRC)/wiredtiger.i
	(cd $(PYSRC) && \
	    $(SWIG) -python -py3 -threads -O -Wall -nodefaultctor -nodefaultdtor -I$(abs_top_builddir) wiredtiger.i && \
	    mkdir -p $(PYTARGET) && \
	    cp wiredtiger/* $(PYTARGET) && \
	    mv wiredtiger.py $(PYTARGET)/__init__.py)

_wiredtiger.so: $(top_builddir)/libwiredtiger.la $(PYSRC)/wiredtiger_wrap.c
	(cd $(PYSRC) && \
	    $(PYTHON) setup.py build_ext -f -b $(abs_builddir) $(PYDIRS))
	(cd $(PYTARGET) && ln -f -s ../_wiredtiger*.so _wiredtiger.so)

install-exec-local:
	(cd $(PYSRC) && \
	    $(PYTHON) setup.py build_py -d $(abs_builddir)/build && \
	    $(PYTHON) setup.py build_ext -f -b $(abs_builddir)/build $(PYDIRS) && \
	    $(PYTHON) setup.py install_lib -b $(abs_builddir)/build --skip-build $(PYTHON_INSTALL_ARG) && \
	    rm -rf $(abs_builddir)/build)

# We build in different places for an install vs running from the tree:
# clean up both.  Don't rely on "setup.py clean" -- everything that should
# be removed is created under the build directory.
clean-local:
	rm -rf build _wiredtiger.so wiredtiger_wrap.o WT_TEST core.* *.core

TESTS = run-ex_access
