#!/usr/bin/env bash
##############################################################################################
# Check branches to ensure backward compatibility.
##############################################################################################

set -e

#############################################################
# format_b_flag:
#       arg1: branch name
#############################################################
format_b_flag()
{
        # Return if the branch's format command takes the -B flag for backward compatibility.
        test "$1" = "develop" && echo "-B "
        test "$1" = "mongodb-4.4" && echo "-B "
}

#############################################################
# build_branch:
#       arg1: branch name
#############################################################
build_branch()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Building branch: \"$1\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

        git clone --quiet https://github.com/wiredtiger/wiredtiger.git "$1"
        cd "$1"
        git checkout --quiet "$1"

        config=""
        config+="--enable-diagnostic "
        config+="--enable-snappy "
        (sh build_posix/reconf &&
            ./configure $config && make -j $(grep -c ^processor /proc/cpuinfo)) > /dev/null
}

#############################################################
# run_format:
#       arg1: branch name
#       arg2: access methods list
#############################################################
run_format()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Running format in branch: \"$1\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

        cd "$1/test/format"

        flags="-1q $(format_b_flag $1)"

        args=""
        args+="cache=80 "                       # Medium cache so there's eviction
        args+="checkpoints=1 "                  # Force periodic writes
        args+="compression=snappy "             # We only built with snappy, force the choice
        args+="data_source=table "
        args+="in_memory=0 "                    # Interested in the on-disk format
        args+="leak_memory=1 "                  # Faster runs
        args+="logging=1 "                      # Test log compatibility
        args+="logging_compression=snappy "     # We only built with snappy, force the choice
        args+="rebalance=0 "                    # Faster runs
        args+="rows=1000000 "
        args+="salvage=0 "                      # Faster runs
        args+="timer=4 "
        args+="verify=0 "                       # Faster runs

        for am in $2; do
            dir="RUNDIR.$am"
            echo "./t running $am access method..."
            ./t $flags -h $dir "file_type=$am" $args

            # Remove the version string from the base configuration file. (MongoDB does not create
            # a base configuration file, but format does, so we need to remove its version string
            # to allow backward compatibility testing.)
            (echo '/^version=/d'
             echo w) | ed -s $dir/WiredTiger.basecfg > /dev/null
        done
}

EXT="extensions=["
EXT+="ext/compressors/snappy/.libs/libwiredtiger_snappy.so,"
EXT+="ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so, "
EXT+="ext/encryptors/rotn/.libs/libwiredtiger_rotn.so, "
EXT+="]"

#############################################################
# verify_branches:
#       arg1: branch name #1
#       arg2: branch name #2
#       arg3: access methods list
#############################################################
verify_branches()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Release \"$1\" verifying \"$2\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

        cd "$1"
        for am in $3; do
            echo "$1/wt verifying $2 access method $am..."
            dir="$2/test/format/RUNDIR.$am"
            WIREDTIGER_CONFIG="$EXT" ./wt -h "../$dir" verify table:wt
        done
}

#############################################################
# upgrade_downgrade:
#       arg1: branch name #1
#       arg2: branch name #2
#       arg3: access methods list
#############################################################
upgrade_downgrade()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Upgrade/downgrade testing with \"$1\" and \"$2\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

        # Alternate running each branch format test program on the second branch's build.
        # Loop twice, that is, run format twice using each branch.
        top="$PWD"
        for am in $3; do
            for reps in {1..2}; do
                echo "$1 format running on $2 access method $am..."
                cd "$top/$1/test/format"
                flags="-1qR $(format_b_flag $1)"
                ./t $flags -h "$top/$2/test/format/RUNDIR.$am" timer=2

                echo "$2 format running on $2 access method $am..."
                cd "$top/$2/test/format"
                flags="-1qR $(format_b_flag $1)"
                ./t $flags -h "RUNDIR.$am" timer=2
            done
        done
}

# Create a directory in which to do the work.
top="test-compatibility-run"
rm -rf "$top" && mkdir "$top"
cd "$top"

# Build the branches.
(build_branch mongodb-3.4)
(build_branch mongodb-3.6)
(build_branch mongodb-4.0)
(build_branch mongodb-4.2)
(build_branch mongodb-4.4)
(build_branch "develop")

# Run format in each branch for supported access methods.
(run_format mongodb-3.4 "fix row var")
(run_format mongodb-3.6 "fix row var")
(run_format mongodb-4.0 "fix row var")
(run_format mongodb-4.2 "fix row var")
(run_format mongodb-4.4 "row")
(run_format "develop" "row")

# Verify backward compatibility for supported access methods.
(verify_branches mongodb-3.6 mongodb-3.4 "fix row var")
(verify_branches mongodb-4.0 mongodb-3.6 "fix row var")
(verify_branches mongodb-4.2 mongodb-4.0 "fix row var")
### (verify_branches mongodb-4.4 mongodb-4.2 "fix row var")
### (verify_branches develop mongodb-4.4 "row")
(verify_branches develop mongodb-4.2 "row")

# Verify forward compatibility for supported access methods.
### (verify_branches mongodb-4.2 mongodb-4.4 "row")
(verify_branches mongodb-4.2 develop "row")
### (verify_branches mongodb-4.4 develop "row")

# Upgrade/downgrade testing for supported access methods.
### (upgrade_downgrade mongodb-4.2 mongodb-4.4 "row")
(upgrade_downgrade mongodb-4.2 develop "row")
### (upgrade_downgrade mongodb-4.4 develop "row")

exit 0
