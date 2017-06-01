#!/bin/bash
#
# workgen_stat.sh - combine JSON time series output from WT and workgen.
#
Usage() {
    cat <<EOF
Usage: $0 [ options ]
Options:
    -h <WT_home_directory>     # set the WiredTiger home directory
    -t2                        # run t2 on the combined files
    -o <output_file>           # output file for result 

At least one of '-t2' or '-o' must be selected.
EOF
    exit 1
}

Filter() {
    sed -e 's/"version" *: *"[^"]*",//' "$@"
}

wthome=.
outfile=
runt2=false

while [ "$#" != 0 ]; do
    arg="$1"
    shift
    case "$arg" in
        -h )
            if [ $# = 0 ]; then
                Usage
            fi
            wthome="$1"
            shift
            ;;
        -o )
            if [ $# = 0 ]; then
                Usage
            fi
            outfile="$1"
            shift
            ;;
        -t2 )
            runt2=true
            ;;
    esac
done
if [ ! -d "$wthome" ]; then
    echo "$wthome: WT home directory does not exist"
    exit 1
fi
if [ ! -f "$wthome/WiredTiger.wt" ]; then
    echo "$wthome: directory is not a WiredTiger home directory"
    exit 1
fi
if [ "$outfile" = '' ]; then
   if [ "$runt2" = false ]; then
       Usage
   fi
   outfile="$wthome/stat_tmp.json"
fi
(cd $wthome; Filter WiredTigerStat.* sample.json) | sort > $outfile
if $runt2; then
    sysname=`uname -s`
    if [ "$sysname" = Darwin ]; then
        open -a t2 $outfile
    else
        t2 $outfile
    fi
fi
