#!/bin/bash
set -o errexit

# Find Python on Windows
if [ "Windows_NT" = "$OS" ]; then
    # Expect to find Python right on C:
    PYTHON_PARENT=/cygdrive/c
    PYTHON_SUBDIR=$(ls -1 $PYTHON_PARENT | grep -i Python3 | sort --version-sort | tail -n 1)
    if [ -z $PYTHON_SUBDIR ]; then
        echo "FATAL: Could not find Python in $PYTHON_PARENT" >&2
        exit 1
    fi
    PYTHON_DIR="$PYTHON_PARENT/$PYTHON_SUBDIR"
    PYTHON_EXE="$PYTHON_DIR/python.exe"
    PYTHON_EXTRA_PATH="$PYTHON_DIR:$PYTHON_DIR/Scripts"

# Find Python on MacOS
elif [ "Darwin" = $(uname) ]; then
    # Check Homebrew first
    PYTHON_PARENT=/opt/homebrew/Frameworks/Python.framework/Versions
    PYTHON_SUBDIR=$(ls -1 $PYTHON_PARENT | grep ^3 | sort --version-sort | tail -n 1)
    if [ ! -z $PYTHON_SUBDIR ]; then
        PYTHON_DIR="$PYTHON_PARENT/$PYTHON_SUBDIR"
        PYTHON_EXE="$PYTHON_DIR/bin/python3"
        PYTHON_EXTRA_PATH="$PYTHON_DIR/bin"
    elif [ ! -x /usr/bin/python3 ]; then
        PYTHON_EXE="/usr/bin/python3"
        # Not supported - no need to figure this out, as it's not currently needed
        PYTHON_DIR=
        # No need to add anything to PATH
        PYTHON_EXTRA_PATH=
    else
        echo "FATAL: Could not find Python in $PYTHON_PARENT or /usr/bin" >&2
        exit 1
    fi

# Expect Python to be in /usr/bin on other platforms
else
    PYTHON_EXE="/usr/bin/python3"
    if [ ! -x $PYTHON_EXE ]; then
        echo "FATAL: Could not find Python in /usr/bin" >&2
        exit 1
    fi
    # Not supported - no need to figure this out, as it's not currently needed
    PYTHON_DIR=
    # No need to add anything to PATH
    PYTHON_EXTRA_PATH=
fi

# Usage info
usage() {
    echo "Usage: find_python.sh OPTIONS" >&2
    echo >&2
    echo "Options:">&2
    echo "  -d  Print Python's directory (only on Mac with Homebrew and on Windows)">&2
    echo "  -h  Print this usage info">&2
    echo "  -p  Print paths to add to \$PATH">&2
    echo "  -w  Use Windows paths">&2
    echo "  -x  Print Python's executable">&2
}

# Optional path transformation for Windows
USE_WINDOWS_PATHS=
path_transform() {
    if [ ! -z $USE_WINDOWS_PATHS ]; then
        tr ':' ';' | sed 's|/cygdrive/c|C:|g' | tr '/' '\\'
    else
        cat
    fi
}

# Depending on the command-line argument, return whatever the caller asks
PRINTED=
while getopts ":dhpwx" opt; do
    case $opt in
        d) PRINTED=1; echo "$PYTHON_DIR" | path_transform ;;
        h) usage; exit 0 ;;
        p) PRINTED=1; echo "$PYTHON_EXTRA_PATH" | path_transform ;;
        w) USE_WINDOWS_PATHS=1 ;;
        x) PRINTED=1; echo "$PYTHON_EXE" | path_transform ;;
        *) usage; exit 1 ;;
    esac
done

shift "$((OPTIND-1))"
if [ ! -z $1 ]; then
    usage
    exit 1
fi

# Print the executable path by default
if [ -z $PRINTED ]; then
    echo "$PYTHON_EXE" | path_transform
fi
