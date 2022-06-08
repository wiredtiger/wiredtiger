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

#ifndef CONFIGURATION_H
#define CONFIGURATION_H

#include <string>
#include <vector>

extern "C" {
#include "wiredtiger.h"
}

namespace test_harness {
inline std::vector<std::string>
SplitString(const std::string &str, const char delim)
{
    std::vector<std::string> splits;
    std::string currentString;
    for (const auto c : str) {
        if (c == delim) {
            if (!currentString.empty()) {
                splits.push_back(currentString);
                currentString.clear();
            }
        } else
            currentString.push_back(c);
    }
    if (!currentString.empty())
        splits.push_back(std::move(currentString));
    return (splits);
}

class Configuration {
    public:
    Configuration(const std::string &testConfigurationName, const std::string &config);
    explicit Configuration(const WT_CONFIG_ITEM &nested);

    ~Configuration();

    /*
     * Wrapper functions for retrieving basic configuration values. Ideally tests can avoid using
     * the config item struct provided by wiredtiger.
     *
     * When getting a configuration value that may not exist for that configuration string or
     * component, the optional forms of the functions can be used. In this case a default value must
     * be passed and it will be set to that value.
     */
    bool GetBool(const std::string &key);
    bool GetOptionalBool(const std::string &key, const bool def);
    int64_t GetInt(const std::string &key);
    int64_t GetOptionalInt(const std::string &key, const int64_t def);
    Configuration *GetSubconfig(const std::string &key);
    Configuration *GetOptionalSubconfig(const std::string &key);
    std::string GetString(const std::string &key);
    std::string GetOptionalString(const std::string &key, const std::string &def);
    std::vector<std::string> GetList(const std::string &key);
    std::vector<std::string> GetOptionalList(const std::string &key);

    /* Get the sleep time from the configuration in ms. */
    uint64_t GetThrottleMs();

    private:
    enum class ConfigurationType { kBool, kInt, kList, kString, kStruct };

    template <typename T>
    T Get(const std::string &key, bool optional, ConfigurationType type, T def,
      T (*func)(WT_CONFIG_ITEM item));

    /*
     * Merge together two configuration strings, the user one and the default one.
     */
    static std::string MergeDefaultConfig(
      const std::string &defaultConfig, const std::string &userConfig);

    /*
     * Split a config string into keys and values, taking care to not split incorrectly when we have
     * a sub config or array.
     */
    static std::vector<std::pair<std::string, std::string>> SplitConfig(const std::string &config);

    static bool Comparator(
      std::pair<std::string, std::string> a, std::pair<std::string, std::string> b);

    private:
    std::string _config;
    WT_CONFIG_PARSER *_configParser = nullptr;
};
} // namespace test_harness

#endif
