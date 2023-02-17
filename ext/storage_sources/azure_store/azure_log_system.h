/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#pragma once
#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <azure/core.hpp>

using namespace Azure::Core::Diagnostics;

static const std::map<int32_t, Logger::Level> wt_to_azure_verbosity_mapping = {
  {WT_VERBOSE_ERROR, Logger::Level::Error},
  {WT_VERBOSE_WARNING, Logger::Level::Warning},
  {WT_VERBOSE_INFO, Logger::Level::Informational},
  {WT_VERBOSE_DEBUG_1, Logger::Level::Verbose},
  {WT_VERBOSE_DEBUG_2, Logger::Level::Verbose},
  {WT_VERBOSE_DEBUG_3, Logger::Level::Verbose},
  {WT_VERBOSE_DEBUG_4, Logger::Level::Verbose},
  {WT_VERBOSE_DEBUG_5, Logger::Level::Verbose},
};

static const std::map<Logger::Level, int32_t> azure_to_wt_verbosity_mapping = {
  {Logger::Level::Error, WT_VERBOSE_ERROR},
  {Logger::Level::Warning, WT_VERBOSE_WARNING},
  {Logger::Level::Informational, WT_VERBOSE_INFO},
  {Logger::Level::Verbose, WT_VERBOSE_DEBUG_5},
};

Logger::Level wt_to_azure_verbosity_level(int32_t wt_verbosity_level);

int32_t azure_to_wt_verbosity_level(Logger::Level);
