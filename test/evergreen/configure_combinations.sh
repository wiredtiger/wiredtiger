#!/bin/bash

: ${CMAKE_BIN:=cmake}

for i in "$@"; do
  case $i in
    -g=*|--generator=*)
      GENERATOR="${i#*=}"
      shift # past argument=value
      ;;
    -j=*|--parallel=*)
      PARALLEL="-j ${i#*=}"
      shift # past argument=value
      ;;
    *)
      # unknown option
      ;;
  esac
done

if [ -z "${GENERATOR}" ]; then
    GENERATOR="Unix Makefiles"
fi
if [ "$GENERATOR" != "Ninja" ] && [ "$GENERATOR" != "Unix Makefiles" ]; then
    echo "Invalid build generator: $GENERATOR. Valid options 'Ninja', 'Unix Makefiles'"
fi

if [ "$GENERATOR" == "Unix Makefiles" ]; then
    GENERATOR=$(echo $GENERATOR | sed -e 's/ /\\ /')
    GENERATOR_CMD="make"
else
    GENERATOR_CMD="ninja"
fi

cd $(git rev-parse --show-toplevel)
echo `pwd`

curdir=`pwd`

compilers=(gcc clang)

options=(
    "-DHAVE_DIAGNOSTIC=ON"
    "-DENABLE_SHARED=OFF -DENABLE_STATIC=ON"
    "-DENABLE_STATIC=OFF -DENABLE_PYTHON=ON"
    "-DENABLE_SNAPPY=ON -DENABLE_ZLIB=ON -DENABLE_LZ4=ON"
    "-DHAVE_BUILTIN_EXTENSION_LZ4=ON -DHAVE_BUILTIN_EXTENSION_SNAPPY=ON -DHAVE_BUILTIN_EXTENSION_ZLIB=ON"
    "-DHAVE_DIAGNOSTIC=ON -DENABLE_PYTHON=ON"
    "-DENABLE_STATIC=ON -DENABLE_SHARED=OFF -DWITH_PIC=ON"
)

always="-DENABLE_STRICT=ON -DENABLE_COLORIZE_OUTPUT=OFF"

saved_IFS=$IFS
cr_IFS="
"

# This function may alter the current directory on failure
BuildTest() {
        local options="$1"
        echo "Building: CC=$CC CXX=$CXX, $options"
        rm -rf ./build || return 1
        mkdir build || return 1
        cd ./build
        eval $CMAKE_BIN "$options" \
                 -DCMAKE_INSTALL_PREFIX="$insdir" -G $GENERATOR ../. || return 1
        eval $GENERATOR_CMD $PARALLEL || return 1
        if [ "$GENERATOR" == "Unix\ Makefiles" ]; then
            $GENERATOR_CMD -C examples/c  VERBOSE=1 > /dev/null || return 1
        else
            $GENERATOR_CMD examples/c/all > /dev/null || return 1
        fi
        eval $GENERATOR_CMD install || return 1
        (echo $options | grep "ENABLE_SHARED=OFF") && wt_build="--static" || wt_build=""
        cflags=`pkg-config wiredtiger $wt_build --cflags --libs`

        echo $CC -o ./smoke ../examples/c/ex_smoke.c $cflags
        $CC -o ./smoke ../examples/c/ex_smoke.c $cflags || return 1
        LD_LIBRARY_PATH="$insdir/lib:$insdir/lib64" ./smoke || return 1
        return 0
}

ecode=0
insdir=`pwd`/installed
export PKG_CONFIG_PATH="$insdir/lib/pkgconfig:$insdir/lib64/pkgconfig"
IFS="$cr_IFS"
for cc in "${compilers[@]}" ; do
        case "$cc" in
            gcc)   cxx=g++ ;;
            clang) cxx=clang++ ;;
            *)     echo "*** ERROR: unknown compiler $cc"; exit 1 ;;
        esac
        export CC=$cc CXX=$cxx
        echo "Using compiler: CC=$CC CXX=$CXX"

        for option in "${options[@]}" ; do
               cd "$curdir"
               IFS="$saved_IFS"
               option="$option $always"
               if ! BuildTest "$option"; then
                       ecode=1
                       echo "*** ERROR: $CC, $option"
               fi
               IFS="$cr_IFS"
       done
done
IFS=$saved_IFS
exit $ecode
