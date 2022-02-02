#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include "s3_log_system.h"

#include <cstdarg>

S3LogSystem::S3LogSystem(WT_EXTENSION_API *wtApi, uint32_t wtVerbosityLevel)
{
    // Mapping the desired WiredTiger extension verbosity level to a rough equivalent AWS SDK
    // verbosity level.
    static const std::map<int32_t, Aws::Utils::Logging::LogLevel> verbosityMapping = {
      {-3, Aws::Utils::Logging::LogLevel::Error}, {-2, Aws::Utils::Logging::LogLevel::Warn},
      {-1, Aws::Utils::Logging::LogLevel::Info}, {0, Aws::Utils::Logging::LogLevel::Info},
      {1, Aws::Utils::Logging::LogLevel::Debug}};
    if (verbosityMapping.find(wtVerbosityLevel) != verbosityMapping.end()) {
        logLevel = verbosityMapping.at(wtVerbosityLevel);
    } else {
        logLevel = Aws::Utils::Logging::LogLevel::Error;
    }
    this->wtApi = wtApi;
}

void
S3LogSystem::Log(Aws::Utils::Logging::LogLevel logLevel, const char *tag, const char *format, ...)
{
    Aws::StringStream ss;
    std::va_list args;
    va_list tmpArgs; // unfortunately you cannot consume a va_list twice
    va_start(args, format);

#ifdef _WIN32
    const int requiredLength = _vscprintf(formatStr, tmp_args) + 1;
#else
    const int requiredLength = vsnprintf(nullptr, 0, format, tmpArgs) + 1;
#endif
    va_end(tmpArgs);

    Aws::Utils::Array<char> outputBuff(requiredLength);
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
S3LogSystem::LogStream(
  Aws::Utils::Logging::LogLevel logLevel, const char *tag, const Aws::OStringStream &messageStream)
{
    LogVerboseMessage(tag, logLevel, messageStream.rdbuf()->str().c_str());
}

void
S3LogSystem::LogVerboseMessage(
  const char *tag, Aws::Utils::Logging::LogLevel logLevel, const std::string &message)
{
    wtApi->err_printf(wtApi, NULL, "%s : %s", tag, message.c_str());
}

void
S3LogSystem::Flush()
{
    return;
}
