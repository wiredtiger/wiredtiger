#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

"""
Check and reformat CMake files using cmake-format.
https://github.com/cheshirekow/cmake_format
"""

import subprocess
import sys
from pathlib import Path
from shutil import which


def _check_cmake_format():
    if not which("cmake-format"):
        print("cmake-format not found. Install it with:")
        print("    pip install cmakelang==0.6.13")
        sys.exit(1)


def _find_cmake_files() -> list[Path]:
    repo_root = Path(__file__).parent.parent

    result = subprocess.run(
        ["git", "ls-files", "*.cmake", "**/CMakeLists.txt", "*.cmake.in"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    return [repo_root / p for p in result.stdout.splitlines()]


def _is_formatted_correctly(path: Path) -> bool:
    result = subprocess.run(
        ["cmake-format", "--check", str(path)],
        capture_output=True,
        check=False,
    )

    return result.returncode == 0


def _format_file(path: Path):
    print(f"Reformatting {path}")

    subprocess.run(
        ["cmake-format", "--in-place", str(path)],
        check=True,
    )


def main() -> int:
    """Format CMake files; return 1 if any were reformatted, else 0."""
    _check_cmake_format()

    cmake_files = _find_cmake_files()
    reformatted = False

    for path in cmake_files:
        if not _is_formatted_correctly(path):
            _format_file(path)
            reformatted = True

    return 1 if reformatted else 0


if __name__ == "__main__":
    sys.exit(main())
