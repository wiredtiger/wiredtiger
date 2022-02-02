#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include "S3StoreLogSystem.h"

#include <cstdarg>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;

S3StoreLogSystem::S3StoreLogSystem(WT_EXTENSION_API *wtApi, uint32_t awsVerbose)
{
    static const std::map<int32_t, Aws::Utils::Logging::LogLevel> verbosityMapping = {
      {-3, Aws::Utils::Logging::LogLevel::Error}, {-2, Aws::Utils::Logging::LogLevel::Warn},
      {-1, Aws::Utils::Logging::LogLevel::Info}, {0, Aws::Utils::Logging::LogLevel::Info},
      {1, Aws::Utils::Logging::LogLevel::Debug}};
    if (verbosityMapping.find(awsVerbose) != verbosityMapping.end()) {
        logLevel = verbosityMapping.at(awsVerbose);
    } else {
        logLevel = Aws::Utils::Logging::LogLevel::Error;
    }
    wt_api = wtApi;
}

void
S3StoreLogSystem::Log(
  Aws::Utils::Logging::LogLevel logLevel, const char *tag, const char *format, ...)
{
    Aws::StringStream ss;
    std::va_list args;
    va_start(args, format);
    va_list tmpArgs; // unfortunately you cannot consume a va_list twice

#ifdef _WIN32
    const int requiredLength = _vscprintf(formatStr, tmp_args) + 1;
#else
    const int requiredLength = vsnprintf(nullptr, 0, format, tmpArgs) + 1;
#endif
    va_end(tmpArgs);

    Array<char> outputBuff(requiredLength);
#ifdef _WIN32
    vsnprintf_s(outputBuff.GetUnderlyingData(), requiredLength, _TRUNCATE, formatStr, args);
#else
    vsnprintf(outputBuff.GetUnderlyingData(), requiredLength, format, args);
#endif // _WIN32

    ss << outputBuff.GetUnderlyingData() << std::endl;

    LogVerboseMessage(tag, logLevel, ss.str());
    va_end(args);
}

void
S3StoreLogSystem::LogStream(
  Aws::Utils::Logging::LogLevel logLevel, const char *tag, const Aws::OStringStream &messageStream)
{
    LogVerboseMessage(tag, logLevel, messageStream.rdbuf()->str().c_str());
}

void
S3StoreLogSystem::LogVerboseMessage(
  const char *tag, Aws::Utils::Logging::LogLevel logLevel, const std::string &message)
{
    wt_api->err_printf(wt_api, NULL, "%s : %s", tag, message.c_str());
}

void
S3StoreLogSystem::Flush()
{
    return;
}
