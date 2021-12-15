#!/usr/bin/env bash
##############################################################################################
# Check branches to ensure forward/backward compatibility, including some upgrade/downgrade testing.
##############################################################################################

set -e

#############################################################
# bflag:
#       arg1: branch name
#############################################################
bflag()
{
    # Return if the branch's format command takes the -B flag for backward compatibility.
    test "$1" = "develop" && echo "-B "
    test "$1" = "mongodb-5.0" && echo "-B "
    test "$1" = "mongodb-4.4" && echo "-B "
    return 0
}

#############################################################
# get_prev_version:
#       arg1: branch name
#############################################################
get_prev_version()
{
    # Sort the list of WiredTiger tags numerically, then pick out the argument number of releases
    # from the end of the list. That is, get a list of releases in numeric order, then pick out
    # the last release (argument "1"), the next-to-last release (argument "2") and so on. Assumes
    # WiredTiger releases are tagged with just numbers and decimal points.
    echo "$(git tag | egrep '^[0-9][0-9.]*$' | sort -g | tail -$1 | head -1)"
}

#############################################################
# get_patch_versions:
#       arg1: branch name
#############################################################
get_patch_versions()
{
    # Query out all released patch versions for a given release branch using "git tag"
    versions=()
    for v in $(git tag | grep $b | grep -v rc)
    do
        versions+=("$v")
    done
}

#############################################################
# pick_a_version:
#       arg1: an array of patch versions
#############################################################
pick_a_version()
{
    # Randomly pick a version from the array of patch versions
    pv=${versions[$RANDOM % ${#versions[@]} ]}
    pversions+=("$pv")
    echo "$pv"
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

    # disable docs compilation in older releases
    sed -i '/man1\|man3/d' build_posix/Make.base

    config=""
    config+="--enable-snappy "
    config+="--disable-standalone-build "
    (sh build_posix/reconf &&
        ./configure $config && make -j $(grep -c ^processor /proc/cpuinfo)) > /dev/null
}

#############################################################
# get_config_file_name:
#       arg1: branch name
#############################################################
get_config_file_name()
{
    local file_name=""
    branch_name=$1
    if [ "${wt_standalone}" = true ] || [ $older = true ] ; then
        file_name="${branch_name}/test/format/CONFIG_default"
        echo $file_name
        return
    fi
    file_name="CONFIG_${branch_name}"

    echo $file_name
}

#############################################################
# create_configs:
#       arg1: branch name
#############################################################
create_configs()
{
    branch_name=$1

    file_name=$(get_config_file_name $branch_name)

    if [ -f $file_name ] ; then
        echo " WARNING - ${file_name} already exists, overwriting it."
    fi

    echo "##################################################" > $file_name
    echo "runs.type=row" >> $file_name              # WT-7379 - Temporarily disable column store tests
    echo "btree.prefix=0" >> $file_name             # WT-7579 - Prefix testing isn't portable between releases
    echo "cache=80" >> $file_name                   # Medium cache so there's eviction
    echo "checksum=on" >> $file_name                # WT-7851 Fix illegal checksum configuration
    echo "checkpoints=1"  >> $file_name             # Force periodic writes
    echo "compression=snappy"  >> $file_name        # We only built with snappy, force the choice
    echo "data_source=table" >> $file_name
    echo "huffman_key=0" >> $file_name              # WT-6893 - Not supoprted by newer releases 
    echo "in_memory=0" >> $file_name                # Interested in the on-disk format
    echo "leak_memory=1" >> $file_name              # Faster runs
    echo "logging=1" >> $file_name                  # Test log compatibility
    echo "logging_compression=snappy" >> $file_name # We only built with snappy, force the choice
    echo "rows=1000000" >> $file_name
    echo "salvage=0" >> $file_name                  # Faster runs
    echo "timer=4" >> $file_name
    echo "verify=1" >> $file_name                   # Faster runs

    # Append older release configs for newer compatibility release test
    if [ $newer = true ]; then
        for i in "${compatible_upgrade_downgrade_release_branches[@]}"
        do
            if [ "$i" == "$branch_name" ] ; then
                echo "transaction.isolation=snapshot" >> $file_name # WT-7545 - Older releases can't do lower isolation levels
                echo "transaction.timestamps=1" >> $file_name       # WT-7545 - Older releases can't do non-timestamp transactions
                break
            fi
        done
    fi
    echo "##################################################" >> $file_name
}

#############################################################
# create_default_configs:
# This function will create the default configs for older and standalone
# release branches.
#############################################################
create_default_configs()
{
    # Iterate over the release branches and create configuration files
    for b in `ls`; do
        if [ -d "$b" ]; then
            (create_configs $b)
            [ -f CONFIG_$b ] && cp -rf CONFIG_$b $b/test/format/
        fi
    done
}

#############################################################
# create_configs_for_newer_release_branches:
#############################################################
create_configs_for_newer_release_branches()
{
    # Create configs for all the newer releases
    for b in ${newer_release_branches[@]}; do
        (create_configs $b)
    done

    # Copy per-release configs in the newer release branches
    for b in ${newer_release_branches[@]}; do
        cp -rf CONFIG* $b/test/format/
    done

    # Delete configs from the top folder
    rm -rf CONFIG*
}

#############################################################
# run_format:
#       arg1: branch name
#       arg2: access methods list
#############################################################
run_format()
{
    branch_name=$1
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Running format in branch: \"$branch_name\""
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

    cd "$branch_name/test/format"
    flags="-1q $(bflag $branch_name)"

    config_file=""

    # Compatibility test for newer releases will have CONFIG file for each release
    # branches for the upgrade/downgrade testing.
    #
    # Compatibility test for older and standalone releases will have the default config.
    if [ "${wt_standalone}" = true ] || [ $older = true ]; then
        config_file="-c CONFIG_default"
    else
        config_file="-c CONFIG_${branch_name}"
    fi

    for am in $2; do
        dir="RUNDIR.$am"
        echo "./t running $am access method..."
        ./t $flags ${config_file} -h $dir "file_type=$am"

        # Remove the version string from the base configuration file. (MongoDB does not create
        # a base configuration file, but format does, so we need to remove its version string
        # to allow backward compatibility testing.)
        (echo '/^version=/d'
            echo w) | ed -s $dir/WiredTiger.basecfg > /dev/null
    done
    cd -
}

#############################################################
# run_test_checkpoint:
#       arg1: branch name
#       arg2: access methods list
#############################################################
run_test_checkpoint()
{
    branch_name=$1
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Running test checkpoint in branch: \"$branch_name\""
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

    cd "$branch_name/test/checkpoint"
    # With the timestamp and prepare transactions configuration, this test
    # can produce a scenario where the on-disk tables have more data than
    # the checkpoint can see.
    #
    # During the verification stage, rollback to stable has to be performed
    # with the checkpoint snapshot to achieve the consistency.
    flags="-W 3 -D -p -x -n 100000 -k 100000 -C cache_size=100MB"

    for am in $2; do
        dir="RUNDIR.$am"
        echo "./t running $am access method..."
        if [ "$am" == "fix" ]; then
            ./t -t f $flags -h $dir
        elif [ "$am" == "var" ]; then
            ./t -t c $flags -h $dir
        else
            ./t -t r $flags -h $dir
        fi
    done
    cd -
}

#############################################################
# run_tests:
#       arg1: branch name
#       arg2: access methods list
#############################################################
run_tests()
{
    run_format $1 $2
    run_test_checkpoint $1 $2
}

EXT="extensions=["
EXT+="ext/compressors/snappy/.libs/libwiredtiger_snappy.so,"
EXT+="ext/collators/reverse/.libs/libwiredtiger_reverse_collator.so, "
EXT+="ext/encryptors/rotn/.libs/libwiredtiger_rotn.so, "
EXT+="]"

#############################################################
# verify_test_format:
#       arg1: branch name #1
#       arg2: branch name #2
#       arg3: access methods list
#       arg4: backward compatibility
#############################################################
verify_test_format()
{
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Release \"$1\" format verifying \"$2\""
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

    cd "$1"
    for am in $3; do
        echo "$1/wt verifying $2 access method $am..."
        dir="$2/test/format/RUNDIR.$am"
        WIREDTIGER_CONFIG="$EXT" ./wt $(bflag $1) -h "../$dir" verify table:wt

        if [ "$4" = true ]; then
            echo "$1/wt dump and load $2 access method $am..."
            WIREDTIGER_CONFIG="$EXT" ./wt $(bflag $1) -h "../$dir" dump table:wt > dump_wt.txt
            WIREDTIGER_CONFIG="$EXT" ./wt $(bflag $1) -h "../$dir" load -f dump_wt.txt
        fi
    done
    cd -
}

#############################################################
# verify_test_checkpoint:
#       arg1: branch name #1
#       arg2: branch name #2
#       arg3: access methods list
#############################################################
verify_test_checkpoint()
{
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Release \"$1\" test checkpoint verifying \"$2\""
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="

    cd "$1"
    for am in $3; do
        echo "$1/test/checkpoint/t verifying $2 access method $am..."
        dir="$2/test/checkpoint/RUNDIR.$am"
        cp -fr "../$dir" "../$dir.backup"
        if [ "$am" = "fix" ]; then
            ./test/checkpoint/t -t f -D -v -h "../$dir"
        elif [ "$am" = "var" ]; then
            ./test/checkpoint/t -t c -D -v -h "../$dir"
        else
            ./test/checkpoint/t -t r -D -v -h "../$dir"
        fi
    done
    cd -
}

#############################################################
# verify_branches:
#       arg1: branch name #1
#       arg2: branch name #2
#       arg3: access methods list
#       arg4: backward compatibility
#############################################################
verify_branches()
{
    verify_test_format $1 $2 $3 $4
    verify_test_checkpoint $1 $2 $3
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

    cfg_file_branch1=$(get_config_file_name $1)
    cfg_file_branch2=$(get_config_file_name $2)

    # Alternate running each branch format test program on the second branch's build.
    # Loop twice, that is, run format twice using each branch.
    top="$PWD"
    for am in $3; do
        for reps in {1..2}; do
            echo "$1 format running on $2 access method $am..."
            cd "$top/$1/test/format"
            flags="-1Rq $(bflag $1)"
            ./t $flags -c "$top/$2/test/format/${cfg_file_branch1}" -h "$top/$2/test/format/RUNDIR.$am" timer=2

            echo "$2 format running on $2 access method $am..."
            cd "$top/$2/test/format"
            flags="-1Rq $(bflag $2)"
            ./t $flags -c $cfg_file_branch2 -h "RUNDIR.$am" timer=2
        done
    done
}

# Only one of below flags will be set by the 1st argument of the script.
older=false
newer=false
wt_standalone=false
patch_version=false

# Branches in below 2 arrays should be put in newer-to-older order.
#
# An overlap (last element of the 1st array & first element of the 2nd array)
# is expected to avoid missing the edge testing coverage.
#
# The 2 arrays should be adjusted over time when newer branches are created,
# or older branches are EOL.
newer_release_branches=(develop mongodb-5.0 mongodb-4.4 mongodb-4.2)
older_release_branches=(mongodb-4.2 mongodb-4.0 mongodb-3.6)

# This array is used to generate compatible configuration files between releases, because
# upgrade/downgrade test runs each build's format test program on the second build's
# configuration file. 
compatible_upgrade_downgrade_release_branches=(mongodb-4.4 mongodb-4.2)

# This array is used to configure the release branches we'd like to run patch version
# upgrade/downgrade test.
patch_version_upgrade_downgrade_release_branches=(mongodb-5.0 mongodb-4.4)

# This array is used to configure the release branches we'd like to run test checkpoint
# upgrade/downgrade test.
test_checkpoint_release_branches=(develop mongodb-5.0 mongodb-4.4)

declare -A scopes
scopes[newer]="newer stable release branches"
scopes[older]="older stable release branches"
scopes[patch_version]="patch versions of the same release branch"
scopes[wt_standalone]="WiredTiger standalone releases"
scopes[two_versions]="any two given versions"

#############################################################
# usage string
#############################################################
usage()
{
    echo -e "Usage: \tcompatibility_test_for_releases [-n|-o|-p|-w|-v]"
    echo -e "\t-n\trun compatibility tests for ${scopes[newer]}"
    echo -e "\t-o\trun compatibility tests for ${scopes[older]}"
    echo -e "\t-p\trun compatibility tests for ${scopes[patch_version]}"
    echo -e "\t-w\trun compatibility tests for ${scopes[wt_standalone]}"
    echo -e "\t-v <v1> <v2>\trun compatibility tests for ${scopes[two_versions]}"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

# Script argument processing
case $1 in
"-n")
    newer=true
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Performing compatibility tests for ${scopes[newer]}"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
;;
"-o")
    older=true
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Performing compatibility tests for ${scopes[older]}"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
;;
"-p")
    patch_version=true
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Performing compatibility tests for ${scopes[patch_version]}"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
;;
"-w")
    wt_standalone=true
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Performing compatibility tests for ${scopes[wt_standalone]}"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
;;
"-v")
    two_versions=true
    v1=$2
    v2=$3
    [[ -z "$v1" || -z "$v2" ]] && usage
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
    echo "Performing compatibility tests for $v1 and $v2"
    echo "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-="
;;
*)
    usage
;;
esac

# Create a directory in which to do the work.
top="test-compatibility-run"
rm -rf "$top" && mkdir "$top"
cd "$top"


if [ "$two_versions" = true ]; then
    # Build the branches
    (build_branch $v1)
    (build_branch $v2)

    # Run test for both branches to generate data files
    (run_test_checkpoint $v1 "row")
    (run_test_checkpoint $v2 "row")

    # Use one version binary to verify data files generated by the other version
    (verify_test_checkpoint "$v1" "$v2" "row")
    (verify_test_checkpoint "$v2" "$v1" "row")

    exit 0
fi

# Build the branches.
if [ "$newer" = true ]; then
    for b in ${newer_release_branches[@]}; do
        (build_branch $b)
    done
fi

if [ "$older" = true ]; then
    for b in ${older_release_branches[@]}; do
        (build_branch $b)
    done
fi

if [ "$patch_version" = true ]; then
    pversions=()
    for b in ${patch_version_upgrade_downgrade_release_branches[@]}; do
        (build_branch $b)
        # Retrieve all released patch versions of the release branch
        cd $b; get_patch_versions; echo $versions; pick_a_version; cd ..
    done
    # Build picked patch version for compatibility test.
    for pv in ${pversions[@]}; do
        (build_branch $pv)
    done
fi

# Get the names of the last two WiredTiger releases, wt1 is the most recent release, wt2 is the
# release before that. Minor trickiness, we depend on the "develop" directory already existing
# so we have a source in which to do git commands.
if [ "${wt_standalone}" = true ]; then
    (build_branch develop)
    cd develop; wt1=$(get_prev_version 1); cd ..
    (build_branch "$wt1")
    cd develop; wt2=$(get_prev_version 2); cd ..
    (build_branch "$wt2")
fi

if [ "$newer" = true ]; then
    create_configs_for_newer_release_branches
else
    create_default_configs
fi

# Run format in each branch for supported access methods.
if [ "$newer" = true ]; then
    for b in ${newer_release_branches[@]}; do
        (run_format $b "row")
    done
    for b in ${test_checkpoint_release_branches[@]}; do
        (run_test_checkpoint $b "row")
    done
fi

if [ "$older" = true ]; then
    for b in ${older_release_branches[@]}; do
        (run_format $b "fix row var")
    done
fi

if [ "${patch_version}" = true ]; then
    for b in ${patch_version_upgrade_downgrade_release_branches[@]}; do
        (run_test_checkpoint "$b" "row")
    done
    for pv in ${pversions[@]}; do
        (run_test_checkpoint "$pv" "row")
    done
fi

if [ "${wt_standalone}" = true ]; then
    (run_tests "$wt1" "row")
    (run_format "$wt2" "row")
fi

# Verify backward compatibility for supported access methods.
#
# The branch array includes a list of branches in newer-to-older order.
# For backport compatibility, the binary of the newer branch should
# be used to verify the data files generated by the older branch.
# e.g. (verify_branches mongodb-4.4 mongodb-4.2 "row")
if [ "$newer" = true ]; then
    for i in ${!newer_release_branches[@]}; do
        [[ $((i+1)) < ${#newer_release_branches[@]} ]] && \
        (verify_test_format ${newer_release_branches[$i]} ${newer_release_branches[$((i+1))]} "row" true)
    done
    for i in ${!test_checkpoint_release_branches[@]}; do
        [[ $((i+1)) < ${#test_checkpoint_release_branches[@]} ]] && \
        (verify_test_checkpoint ${test_checkpoint_release_branches[$i]} ${test_checkpoint_release_branches[$((i+1))]} "row")
    done
fi

if [ "$older" = true ]; then
    for i in ${!older_release_branches[@]}; do
        [[ $((i+1)) < ${#older_release_branches[@]} ]] && \
        (verify_test_format ${older_release_branches[$i]} ${older_release_branches[$((i+1))]} "fix row var" true)
    done
fi

if [ "${patch_version}" = true ]; then
    for b in ${patch_version_upgrade_downgrade_release_branches[@]}; do
        for pv in ${pversions[@]}; do
            (verify_test_checkpoint "$b" "$pv" "row")
        done
    done
fi

if [ "${wt_standalone}" = true ]; then
    (verify_branches develop "$wt1" "row" true)
    (verify_test_format "$wt1" "$wt2" "row" true)
fi

# Verify forward compatibility for supported access methods.
#
# The branch array includes a list of branches in newer-to-older order.
# For forward compatibility, the binary of the older branch should
# be used to verify the data files generated by the newer branch.
# e.g. (verify_branches mongodb-4.2 mongodb-4.4 "row")
if [ "$newer" = true ]; then
    for i in ${!newer_release_branches[@]}; do
        [[ $((i+1)) < ${#newer_release_branches[@]} ]] && \
        (verify_test_format ${newer_release_branches[$((i+1))]} ${newer_release_branches[$i]} "row" false)
    done
    for i in ${!test_checkpoint_release_branches[@]}; do
        [[ $((i+1)) < ${#test_checkpoint_release_branches[@]} ]] && \
        (verify_test_checkpoint ${test_checkpoint_release_branches[$((i+1))]} ${test_checkpoint_release_branches[$i]} "row")
    done
fi

# Upgrade/downgrade testing for supported access methods.
if [ "$newer" = true ]; then
    for i in ${!newer_release_branches[@]}; do
        [[ $((i+1)) < ${#newer_release_branches[@]} ]] && \
        (upgrade_downgrade ${newer_release_branches[$((i+1))]} ${newer_release_branches[$i]} "row")
    done
fi

exit 0
