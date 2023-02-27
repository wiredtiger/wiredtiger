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

#include "azure_log_system.h"

// Constructor for azure_log_system that calls to set the WiredTiger verbosity level.
azure_log_system::azure_log_system(WT_EXTENSION_API *wt_api, uint32_t wt_verbosity_level)
    : _wt_api(wt_api)
{
    set_wt_verbosity_level(wt_verbosity_level);
}

const Azure::Core::Diagnostics::Logger::Level
wt_to_azure_verbosity_level(int32_t wt_verbosity_level)
{
    if (wt_to_azure_verbosity_mapping.find(wt_verbosity_level) !=
      wt_to_azure_verbosity_mapping.end())
        return wt_to_azure_verbosity_mapping.at(wt_verbosity_level);
    else
        return Azure::Core::Diagnostics::Logger::Level::Error;
}

const int32_t
azure_to_wt_verbosity_level(Azure::Core::Diagnostics::Logger::Level azure_verbosity_level)
{
    if (azure_to_wt_verbosity_mapping.find(azure_verbosity_level) !=
      azure_to_wt_verbosity_mapping.end())
        return azure_to_wt_verbosity_mapping.at(azure_verbosity_level);
    else
        return WT_VERBOSE_ERROR;
}

// Sets the WiredTiger verbosity level by mapping the Azure SDK log level.
void
azure_log_system::set_wt_verbosity_level(int32_t wt_verbosity_level)
{
    _wt_verbosity_level = wt_verbosity_level;
    // If the verbosity level is out of range it will default to Azure SDK Error level.
    _azure_log_level = wt_to_azure_verbosity_level(wt_verbosity_level);
    // if (wt_to_azure_verbosity_mapping.find(wt_verbosity_level) !=
    //   wt_to_azure_verbosity_mapping.end())
    //     _azure_log_level = wt_to_azure_verbosity_mapping.at(wt_verbosity_level);
    // else
    //     _azure_log_level = Azure::Core::Diagnostics::Logger::Level::Error;
}

void
azure_log_system::log_verbose_message(int32_t verbosity_level, const std::string &message) const
{
    if (verbosity_level <= _wt_verbosity_level) {
        if (verbosity_level < WT_VERBOSE_NOTICE)
            _wt_api->err_printf(_wt_api, NULL, "%s", message.c_str());
        else
            _wt_api->msg_printf(_wt_api, NULL, "%s", message.c_str());
    }
}

void
azure_log_system::log_err_msg(const std::string &message) const
{
    log_verbose_message(WT_VERBOSE_ERROR, message);
}

void
azure_log_system::log_debug_message(const std::string &message) const
{
    log_verbose_message(WT_VERBOSE_DEBUG_1, message);
}
