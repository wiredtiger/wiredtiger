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
Simple logging module for py_common.
"""

from enum import IntEnum


class LogLevel(IntEnum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3


class Logger:
    """Logger with four log levels: DEBUG, INFO, WARN, ERROR."""

    def __init__(self, level=LogLevel.INFO):
        self._level = level

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, level):
        self._level = level

    def _log(self, level, message):
        if level >= self._level:
            prefix = level.name
            print(f'[{prefix}] {message}')

    def debug(self, message):
        self._log(LogLevel.DEBUG, message)

    def info(self, message):
        self._log(LogLevel.INFO, message)

    def warn(self, message):
        self._log(LogLevel.WARN, message)

    def error(self, message):
        self._log(LogLevel.ERROR, message)


# Global logger instance
_logger = Logger()


def set_level(level):
    """Set the global log level."""
    _logger.level = level


def debug(message):
    """Log a debug message."""
    _logger.debug(message)


def info(message):
    """Log an info message."""
    _logger.info(message)


def warn(message):
    """Log a warning message."""
    _logger.warn(message)


def error(message):
    """Log an error message."""
    _logger.error(message)
