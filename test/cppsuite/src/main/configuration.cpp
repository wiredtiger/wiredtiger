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

#include "configuration.h"

#include <algorithm>
#include <stack>

#include "src/common/constants.h"
#include "src/common/logger.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {
/* Static methods implementation. */
static bool
ConfigItemToBool(const WT_CONFIG_ITEM item)
{
    return (item.val != 0);
}

static int64_t
ConfigItemToInt(const WT_CONFIG_ITEM item)
{
    return (item.val);
}

static std::string
ConfigItemToString(const WT_CONFIG_ITEM item)
{
    return std::string(item.str, item.len);
}

static std::vector<std::string>
ConfigItemToList(const WT_CONFIG_ITEM item)
{
    auto str = ConfigItemToString(item);

    /* Get rid of the brackets. */
    testutil_assert(!str.empty() && str.front() == '[' && str.back() == ']');
    str.pop_back();
    str.erase(0, 1);

    return (SplitString(str, ','));
}

Configuration::Configuration(const std::string &testConfigName, const std::string &config)
{
    const auto *configEntry = __wt_test_config_match(testConfigName.c_str());
    if (configEntry == nullptr)
        testutil_die(EINVAL, "failed to match test config name");
    std::string default_config = std::string(configEntry->base);
    /* Merge in the default configuration. */
    _config = MergeDefaultConfig(default_config, config);
    Logger::LogMessage(LOG_INFO, "Full config: " + _config);

    int ret =
      wiredtiger_test_config_validate(nullptr, nullptr, testConfigName.c_str(), _config.c_str());
    if (ret != 0)
        testutil_die(EINVAL, "failed to validate given config, ensure test config exists");
    ret = wiredtiger_config_parser_open(nullptr, _config.c_str(), _config.size(), &_configParser);
    if (ret != 0)
        testutil_die(EINVAL, "failed to create configuration parser for provided config");
}

Configuration::Configuration(const WT_CONFIG_ITEM &nested)
{
    if (nested.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT)
        testutil_die(EINVAL, "provided config item isn't a structure");
    int ret = wiredtiger_config_parser_open(nullptr, nested.str, nested.len, &_configParser);
    if (ret != 0)
        testutil_die(EINVAL, "failed to create configuration parser for provided sub config");
}

Configuration::~Configuration()
{
    if (_configParser != nullptr) {
        _configParser->close(_configParser);
        _configParser = nullptr;
    }
}

std::string
Configuration::GetString(const std::string &key)
{
    return Get<std::string>(key, false, types::kString, "", ConfigItemToString);
}

std::string
Configuration::GetOptionalString(const std::string &key, const std::string &def)
{
    return Get<std::string>(key, true, types::kString, def, ConfigItemToString);
}

bool
Configuration::GetBool(const std::string &key)
{
    return Get<bool>(key, false, types::kBool, false, ConfigItemToBool);
}

bool
Configuration::GetOptionalBool(const std::string &key, const bool def)
{
    return Get<bool>(key, true, types::kBool, def, ConfigItemToBool);
}

int64_t
Configuration::GetInt(const std::string &key)
{
    return Get<int64_t>(key, false, types::kInt, 0, ConfigItemToInt);
}

int64_t
Configuration::GetOptionalInt(const std::string &key, const int64_t def)
{
    return Get<int64_t>(key, true, types::kInt, def, ConfigItemToInt);
}

Configuration *
Configuration::GetSubconfig(const std::string &key)
{
    return Get<Configuration *>(key, false, types::kStruct, nullptr,
      [](WT_CONFIG_ITEM item) { return new Configuration(item); });
}

Configuration *
Configuration::GetOptionalSubconfig(const std::string &key)
{
    return Get<Configuration *>(key, true, types::kStruct, nullptr,
      [](WT_CONFIG_ITEM item) { return new Configuration(item); });
}

std::vector<std::string>
Configuration::GetList(const std::string &key)
{
    return Get<std::vector<std::string>>(key, false, types::kList, {}, ConfigItemToList);
}

template <typename T>
T
Configuration::Get(
  const std::string &key, bool optional, types type, T def, T (*func)(WT_CONFIG_ITEM item))
{
    WT_DECL_RET;
    WT_CONFIG_ITEM value = {"", 0, 1, WT_CONFIG_ITEM::WT_CONFIG_ITEM_BOOL};

    ret = _configParser->get(_configParser, key.c_str(), &value);
    if (ret == WT_NOTFOUND && optional)
        return (def);
    else if (ret != 0)
        testutil_die(ret, ("Error while finding config with key \"" + key + "\"").c_str());

    const char *error = "Configuration value doesn't match requested type";
    if (type == types::kString &&
      (value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRING &&
        value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_ID))
        testutil_die(-1, error);
    else if (type == types::kBool && value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_BOOL)
        testutil_die(-1, error);
    else if (type == types::kInt && value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_NUM)
        testutil_die(-1, error);
    else if (type == types::kStruct && value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT)
        testutil_die(-1, error);
    else if (type == types::kList && value.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRUCT)
        testutil_die(-1, error);

    return func(value);
}

uint64_t
Configuration::GetThrottleMs()
{
    uint64_t multiplier = 0;
    const std::string throttleConfig(GetOptionalString(kOpRate, "1s"));
    /*
     * Find the ms, s, or m in the string. Searching for "ms" first as the following two searches
     * would match as well.
     */
    size_t pos = throttleConfig.find("ms");
    if (pos != std::string::npos)
        multiplier = 1;
    else {
        pos = throttleConfig.find("s");
        if (pos != std::string::npos)
            multiplier = 1000;
        else {
            pos = throttleConfig.find("m");
            if (pos != std::string::npos)
                multiplier = 60 * 1000;
            else
                testutil_die(-1, "no rate specifier given");
        }
    }
    const std::string magnitude = throttleConfig.substr(0, pos);
    /* This will throw if it can't cast, which is fine. */
    return std::stoi(magnitude) * multiplier;
}

std::string
Configuration::MergeDefaultConfig(const std::string &default_config, const std::string &user_config)
{
    std::string mergedConfig;
    auto splitDefaultConfig = SplitConfig(default_config);
    auto splitUserConfig = SplitConfig(user_config);
    auto userIt = splitUserConfig.begin();
    for (auto defaultIt = splitDefaultConfig.begin(); defaultIt != splitDefaultConfig.end();
         ++defaultIt) {
        if (userIt == splitUserConfig.end() || userIt->first != defaultIt->first)
            /* The default does not exist in the user configuration, add it. */
            mergedConfig += defaultIt->first + "=" + defaultIt->second;
        else {
            /* If we have a sub config merge it in. */
            if (userIt->second[0] == '(')
                mergedConfig += defaultIt->first + "=(" +
                  MergeDefaultConfig(defaultIt->second, userIt->second) + ')';
            else
                /* Add the user configuration as it exists. */
                mergedConfig += userIt->first + "=" + userIt->second;
            ++userIt;
        }
        /* Add a comma after every item we add except the last one. */
        if (splitDefaultConfig.end() - defaultIt != 1)
            mergedConfig += ",";
    }
    /* Add any remaining user config items. */
    while (userIt != splitUserConfig.end()) {
        mergedConfig += "," + userIt->first + "=" + userIt->second;
        ++userIt;
    }
    return (mergedConfig);
}

std::vector<std::pair<std::string, std::string>>
Configuration::SplitConfig(const std::string &config)
{
    std::string cutConfig = config;
    std::vector<std::pair<std::string, std::string>> SplitConfig;
    std::string key = "", value = "";
    bool insideSubconfig = false;
    bool expectValue = false;
    std::stack<char> parens;

    /* All configuration strings must be at least 2 characters. */
    testutil_assert(config.size() > 1);

    /* Remove prefix and trailing "()". */
    if (config[0] == '(')
        cutConfig = config.substr(1, config.size() - 2);

    size_t start = 0, len = 0;
    for (size_t i = 0; i < cutConfig.size(); ++i) {
        if (cutConfig[i] == '(' || cutConfig[i] == '[') {
            parens.push(cutConfig[i]);
            insideSubconfig = true;
        }
        if (cutConfig[i] == ')' || cutConfig[i] == ']') {
            parens.pop();
            insideSubconfig = !parens.empty();
        }
        if (cutConfig[i] == '=' && !insideSubconfig) {
            if (len == 0) {
                testutil_die(EINVAL, "error parsing config: detected empty key");
            }
            if (expectValue) {
                testutil_die(EINVAL,
                  "error parsing config: syntax error parsing value for key ['%s']: '%s'",
                  key.c_str(), cutConfig.substr(start, len).c_str());
            }
            expectValue = true;
            key = cutConfig.substr(start, len);
            start += len + 1;
            len = 0;
            continue;
        }
        if (cutConfig[i] == ',' && !insideSubconfig) {
            if (len == 0) {
                testutil_die(
                  EINVAL, "error parsing config: detected empty value for key:'%s'", key.c_str());
            }
            if (!expectValue) {
                testutil_die(EINVAL,
                  "error parsing config: syntax error parsing key value pair: '%s'",
                  cutConfig.substr(start, len).c_str());
            }
            expectValue = false;
            if (start + len >= cutConfig.size())
                break;
            value = cutConfig.substr(start, len);
            start += len + 1;
            len = 0;
            SplitConfig.push_back(std::make_pair(key, value));
            continue;
        }
        ++len;
    }
    if (expectValue) {
        value = cutConfig.substr(start, len);
        SplitConfig.push_back(std::make_pair(key, value));
    }

    /* We have to sort the config here otherwise we will match incorrectly while merging. */
    std::sort(SplitConfig.begin(), SplitConfig.end(), Comparator);
    return (SplitConfig);
}

bool
Configuration::Comparator(
  std::pair<std::string, std::string> a, std::pair<std::string, std::string> b)
{
    return (a.first < b.first);
}
} // namespace test_harness
