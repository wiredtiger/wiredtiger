import os, sys

# Setup library paths
def setupLib():
    if sys.version_info[0] <= 2:
        print('WiredTiger requires Python version 3.0 or above')
        sys.exit(1)

    # Set paths
    suitedir = sys.path[0]
    wt_disttop = os.path.dirname(os.path.dirname(suitedir))
    wt_3rdpartydir = os.path.join(wt_disttop, 'test', '3rdparty')

    # Check for a local build that contains the wt utility. First check in
    # current working directory, then in build_posix and finally in the disttop
    # directory. This isn't ideal - if a user has multiple builds in a tree we
    # could pick the wrong one. We also need to account for the fact that there
    # may be an executable 'wt' file the build directory and a subordinate .libs
    # directory.
    env_builddir = os.getenv('WT_BUILDDIR')
    curdir = os.getcwd()
    if env_builddir and os.path.isfile(os.path.join(env_builddir, 'wt')):
        wt_builddir = env_builddir
    elif os.path.basename(curdir) == '.libs' and \
    os.path.isfile(os.path.join(curdir, os.pardir, 'wt')):
        wt_builddir = os.path.join(curdir, os.pardir)
    elif os.path.isfile(os.path.join(curdir, 'wt')):
        wt_builddir = curdir
    elif os.path.isfile(os.path.join(curdir, 'wt.exe')):
        wt_builddir = curdir
    elif os.path.isfile(os.path.join(wt_disttop, 'wt')):
        wt_builddir = wt_disttop
    elif os.path.isfile(os.path.join(wt_disttop, 'build_posix', 'wt')):
        wt_builddir = os.path.join(wt_disttop, 'build_posix')
    elif os.path.isfile(os.path.join(wt_disttop, 'wt.exe')):
        wt_builddir = wt_disttop
    else:
        print('Unable to find useable WiredTiger build')
        sys.exit(1)

    # Cannot import wiredtiger and supporting utils until we set up paths
    # We want our local tree in front of any installed versions of WiredTiger.
    # Don't change sys.path[0], it's the dir containing the invoked python script.

    sys.path.insert(1, os.path.join(wt_builddir, 'lang', 'python'))

    # Append to a colon separated path in the environment
    def append_env_path(name, value):
        path = os.environ.get(name)
        if path == None:
            v = value
        else:
            v = path + ':' + value
        os.environ[name] = v

    # If we built with libtool, explicitly put its install directory in our library
    # search path. This only affects library loading for subprocesses, like 'wt'.
    libsdir = os.path.join(wt_builddir, '.libs')
    if os.path.isdir(libsdir):
        append_env_path('LD_LIBRARY_PATH', libsdir)
        if sys.platform == "darwin":
            append_env_path('DYLD_LIBRARY_PATH', libsdir)

    # Add all 3rd party directories: some have code in subdirectories
    #for d in os.listdir(wt_3rdpartydir):
    #    for subdir in ('lib', 'python', ''):
    #        if os.path.exists(os.path.join(wt_3rdpartydir, d, subdir)):
    #            sys.path.insert(1, os.path.join(wt_3rdpartydir, d, subdir))
    #            break
