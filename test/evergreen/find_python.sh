#!/bin/bash
set -o errexit

# Find Python on Windows
if [ "Windows_NT" = "$OS" ]; then
    # Expect to find Python right on C:
    PYTHON_PARENT=/cygdrive/c
    PYTHON_SUBDIR=$(ls -1 PYTHON_PARENT | grep -i Python3 | sort -n | tail -n 1)
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
    PYTHON_SUBDIR=$(ls -1 $PYTHON_PARENT | grep -i ^3 | sort -n | tail -n 1)
    if [ ! -z $PYTHON_SUBDIR ]; then
        PYTHON_DIR="$PYTHON_PARENT/$PYTHON_SUBDIR"
        PYTHON_EXE="$PYTHON_DIR/bin/python3"
        PYTHON_EXTRA_PATH="$PYTHON_DIR/bin"
    elif [ ! -x /usr/bin/python3 ]; then
        PYTHON_EXE="/usr/bin/python3"
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
    # No need to add anything to PATH
    PYTHON_EXTRA_PATH=
fi

# Usage info
usage() {
    echo "Usage: find_python.sh OPTIONS" >&2
    echo >&2
    echo "Options:">&2
    echo "  -h  Print this usage info">&2
    echo "  -p  Print paths to add to \$PATH">&2
    echo "  -x  Print Python's executable">&2
}

# Depending on the command-line argument, return whatever the caller asks
while getopts ":hpx" opt; do
    case $opt in
        h) usage; exit 0 ;;
        p) echo "$PYTHON_EXTRA_PATH"; exit 0 ;;
        x) echo "$PYTHON_EXE"; exit 0 ;;
        *) usage; exit 1 ;;
    esac
done

shift "$((OPTIND-1))"
if [ ! -z $1 ]; then
    usage
    exit 1
fi

# Print the executable path by default
echo "$PYTHON_EXE"
