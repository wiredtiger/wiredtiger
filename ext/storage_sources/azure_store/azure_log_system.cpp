
#include "azure_log_system.h"
#include "azure_storage_source.cpp"
#include <azure/core/diagnostics/logger.hpp>

using namespace Azure::Core::Diagnostics;

Logger::Level
wt_to_azure_verbosity_level(int32_t wt_verbosity_level)
{
    Logger::Level azure_log_level = Azure::Core::Diagnostics::Logger::Level::Error;

    if (wt_to_azure_verbosity_mapping.find(wt_verbosity_level) !=
      wt_to_azure_verbosity_mapping.end())
        azure_log_level = wt_to_azure_verbosity_mapping.at(wt_verbosity_level);

    return azure_log_level;
}

int32_t
azure_to_wt_verbosity_level(Logger::Level azure_verbosity_level)
{
    int32_t wt_verbosity_level = WT_VERBOSE_ERROR;
    if (azure_to_wt_verbosity_mapping.find(azure_verbosity_level) !=
      azure_to_wt_verbosity_mapping.end())
        wt_verbosity_level = azure_to_wt_verbosity_mapping.at(azure_verbosity_level);

    return wt_verbosity_level;
}

static void
log_verbose_message(azure_store azure_store, int32_t verbosity_level, const std::string &message)
{
    // if (verbosity_level <= azure_store->verbose) {
    //     if (verbosity_level < WT_VERBOSE_NOTICE)
    //         azure_store->wt_api->err_printf(azure_store->wt_api, NULL, "%s", message.c_str());
    //     else
    //         azure_store->wt_api->msg_printf(azure_store->wt_api, NULL, "%s", message.c_str());
    // }
}

void
log_err_msg(const std::string &message) {
    //log_verbose_message
}

void
log_debug_message(const std::string &message) {

}
