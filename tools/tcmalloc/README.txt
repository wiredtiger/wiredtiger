WiredTiger utilizes tcmalloc in testing through LD_PRELOAD.

Implicit
========

To load tcmalloc into your current environment run:

export LD_PRELOAD=$PWD/TCMALLOC_LIB/libtcmalloc.so

This will be in effect until you exit the current shell.

NOTE: This will affect ALL binaries run in that environment that
dynamically link to libc.

Explicit
========

Alternatively you can source the script with_tcmalloc.sh in this
directory, like this:

source $PWD/with_tcmalloc.sh

This will define a shell function "with_tcmalloc" that you prefix
commands with to define LD_PRELOAD for that single invocation ONLY.

For example:

 $ with_tcmalloc ./wt -h

Which is equvialent to:

 $ LD_PRELOAD=$PWD/libtcmalloc.so ./wt -h

Debugging
=========

For debugging in gdb you can use $PWD/TCMALLOC_LIB/tcmalloc.gdb

On the command line:

 $ gdb -x $PWD/TCMALLOC_LIB/tcmalloc.gdb wt

Or from within gdb:

 (gdb) source $PWD/TCMALLOC_LIB/tcmalloc.gdb
