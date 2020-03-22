#!/usr/bin/env bash
##############################################################################################
# Check releases to ensure backward compatibility.
##############################################################################################

set -e

###########################################################################
# Return the most recent version of a tagged release.
###########################################################################
get_tagged_release()
{
        echo "$(git tag | grep "^mongodb-$1.[0-9]" | sort -V | sed -e '$p' -e d)"
}

#############################################################
# build_release:
#       arg1: release
#############################################################
build_release()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Building release: \"$1\""
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
#       arg1: release
#       arg2: access methods list
#############################################################
run_format()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Running format in release: \"$1\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

        cd "$1/test/format"

        args=""
        args+="cache=80 "                       # Medium cache so there's eviction
        args+="checkpoints=1 "                  # Force periodic writes
        args+="compression=snappy "             # We only built with snappy, force the choice
        args+="data_source=table "
        args+="in_memory=0 "                    # Interested in the on-disk format
        args+="leak_memory=1 "                  # Faster runs
        args+="logging_compression=snappy "     # We only built with snappy, force the choice
        args+="rebalance=0 "                    # Faster runs
        args+="rows=1000000 "
        args+="salvage=0 "                      # Faster runs
        args+="timer=4 "
        args+="verify=0 "                       # Faster runs

        for am in $2; do
            dir="RUNDIR.$am"
            echo "./t running $am access method..."
            ./t -1q -h $dir "file_type=$am" $args
        done
}

EXT="extensions=["
EXT+="ext/compressors/snappy/.libs/libwiredtiger_snappy.so,"
EXT+="ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so, "
EXT+="ext/encryptors/rotn/.libs/libwiredtiger_rotn.so, "
EXT+="]"

#############################################################
# verify_backward:
#       arg1: release #1
#       arg2: release #2
#       arg3: access methods list
#############################################################
verify_backward()
{
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        echo "Release \"$1\" verifying \"$2\""
        echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
        
        cd "$1"
        for am in $3; do
            dir="$2/test/format/RUNDIR.$am"
            echo "$1/wt verifying $2 access method $am..."

            WIREDTIGER_CONFIG="$EXT" ./wt -h "../$dir" verify table:wt
        done
}

# Create a directory in which to do the work.
top="test-compatibility-run"
rm -rf "$top" && mkdir "$top"
cd "$top"

# Build a release with a known name, then use it to figure out tagged release names.
(build_release "develop")

# Figure out the names of the releases we'll compare.
cd develop
rel34="$(get_tagged_release "3.4")"
rel36="$(get_tagged_release "3.6")"
rel40="$(get_tagged_release "4.0")"
#rel42="$(get_tagged_release "4.2")"    XXX cannot handle 4.4 formats yet.
rel42="mongodb-4.2"
cd ..

# Build the rest of the releases.
(build_release "$rel34")
(build_release "$rel36")
(build_release "$rel40")
(build_release "$rel42")

# Run format in each release for supported access methods.
(run_format "$rel34" "fix row var")
(run_format "$rel36" "fix row var")
(run_format "$rel40" "fix row var")
(run_format "$rel42" "fix row var")
(run_format "develop" "row")

# Verify backward compatibility for supported access methods.
(verify_backward $rel36 "$rel34" "fix row var")
(verify_backward $rel40 "$rel36" "fix row var")
(verify_backward $rel42 "$rel40" "fix row var")
(verify_backward "develop" "$rel42" "row")

exit 0
