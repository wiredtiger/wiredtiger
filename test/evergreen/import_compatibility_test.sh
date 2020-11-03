#!/usr/bin/env bash
#
# Test importing of files created in previous versions of WiredTiger.
# Test that we do not allowing downgrading of databases that have an imported table.

set -e

# build_branch --
#     arg1: branch name
build_branch()
{
    git clone https://github.com/wiredtiger/wiredtiger.git "$1"
    cd "$1"
    git checkout --quiet "$1"
    config=""
    config+="--enable-snappy "
    (sh build_posix/reconf &&
         ./configure $config && make -j $(grep -c ^processor /proc/cpuinfo)) > /dev/null
    cd ..
}

# create_file --
#     arg1: branch name
#     arg2: file name
create_file()
{
    wt_cmd="$1/wt"
    wt_dir="$1/WT_TEST/"
    mkdir -p $wt_dir

    # Create the file and populate with a few key/values.
    $wt_cmd -h $wt_dir create -c "key_format=S,value_format=S" "file:$2"
    $wt_cmd -h $wt_dir write "file:$2" abc 123 def 456 hij 789
}

# import_file --
#     arg1: dest branch name
#     arg2: source branch name
#     arg3: file name
import_file()
{
    wt_cmd="$1/wt"
    wt_dir="$1/WT_TEST/"
    mkdir -p $wt_dir

    # Move the file across.
    import_file="$2/WT_TEST/$3"
    cp $import_file $wt_dir

    # Run import via the wt tool.
    $wt_cmd -h $wt_dir create -c "import=(enabled,repair=true)" "file:$3"
}

# verify_file --
#     arg1: branch name
#     arg2: file name
verify_file()
{
    wt_cmd="$1/wt"
    wt_dir="$1/WT_TEST/"

    $wt_cmd -h $wt_dir verify "file:$2"
}

# build_branch develop
# build_branch mongodb-4.4
create_file mongodb-4.4 test_import
import_file develop mongodb-4.4 test_import
verify_file develop test_import

# Now try to import it back.
#
# We should get this to fail.
mongodb-4.4/wt -h develop/WT_TEST/ dump file:test_import
