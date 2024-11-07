#!/bin/bash

. `dirname -- ${BASH_SOURCE[0]}`/dist/common_functions.sh

SRCDIR="$1"
DSTDIR="$2"

# echo "===" "$@"

[[ -z "$SRCDIR" || -z "$DSTDIR" ]] && exit 1

shift
shift

cp -a $SRCDIR/src $DSTDIR/

# for f in "$@"; do
#     # echo ${SRCDIR}/$f --- $DSTDIR/$f
#     echo "#line 1 \"${SRCDIR}/$f\"" > $DSTDIR/$f
#     cat ${SRCDIR}/$f >> $DSTDIR/$f
# done

cd_top
cd dist/access_check
. ./init.sh
# dist/access_check/qqq.py "$SRCDIR" "$DSTDIR" $@
./qqq.py "$DSTDIR"

