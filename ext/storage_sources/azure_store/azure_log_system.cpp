
#include "azure_log_system.h"
#include <azure/core/diagnostics/logger.hpp>

// Constructor for azure_log_system that calls to set the WiredTiger verbosity level.
azure_log_system::azure_log_system(WT_EXTENSION_API *wt_api, uint32_t wt_verbosity_lvl)
    : _wt_api(wt_api)
{
    set_wt_verbosity_lvl(wt_verbosity_lvl);
}


// Directs the message to WiredTiger's log streams.
void
azure_log_system::log_azure_msg(const std::string &message) const
{
    return;
}

// SetListener wrapper level, message
//

// Directs the message to WiredTiger's log streams matched at WiredTiger's log stream levels.
//  send to wt
// called in init
void
azure_log_system::log_verbose_msg(int32_t verbosity_level, const std::string &message) const
{
    //Azure::Core::Diagnostics::Logger::SetListener([&](auto level, auto msg) {
    //     if (level <= _azure_log_level) {
    //         if (level < WT_VERBOSE_NOTICE)
    //             _wt_api->err_printf(_wt_api, NULL, "%s", msg.c_str());
    //         else
    //             _wt_api->msg_printf(_wt_api, NULL, "%s", msg.c_str());
    //     }
    // });

    return;
}

// Sets the WiredTiger Extension's verbosity level and matches the Azure log levels to this.
void
azure_log_system::set_wt_verbosity_lvl(int32_t wt_verbosity_lvl)
{
    //Azure::Core::Diagnostics::Logger azure_logger;
    _wt_verbosity_lvl = wt_verbosity_lvl;
    if (verbosity_mapping.find(_wt_verbosity_lvl) != verbosity_mapping.end())
        _azure_log_level = verbosity_mapping.at(_wt_verbosity_lvl);
    else
        _azure_log_level = Azure::Core::Diagnostics::Logger::Level::Error;
}
