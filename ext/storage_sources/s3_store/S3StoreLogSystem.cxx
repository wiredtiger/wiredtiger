#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#include "S3StoreLogSystem.h"

#include <cstdarg>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;

S3StoreLogSystem::S3StoreLogSystem(){
    log_level = Aws::Utils::Logging::LogLevel::Info;
}

void S3StoreLogSystem::Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format, ...)
{
    Aws::StringStream ss;
    std::va_list args;
    va_start(args, format);
    va_list tmp_args; //unfortunately you cannot consume a va_list twice

    #ifdef _WIN32
        const int requiredLength = _vscprintf(formatStr, tmp_args) + 1;
    #else
        const int requiredLength = vsnprintf(nullptr, 0, format, tmp_args) + 1;
    #endif
    va_end(tmp_args);

    Array<char> outputBuff(requiredLength);
    #ifdef _WIN32
        vsnprintf_s(outputBuff.GetUnderlyingData(), requiredLength, _TRUNCATE, formatStr, args);
    #else
        vsnprintf(outputBuff.GetUnderlyingData(), requiredLength, format, args);
    #endif // _WIN32

    ss << outputBuff.GetUnderlyingData() << std::endl;  

    LogVerboseMessage(tag, log_level, ss.str());
    va_end(args);
}

void S3StoreLogSystem::LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag, const Aws::OStringStream& message_stream) {
    LogVerboseMessage(tag, log_level, message_stream.rdbuf()->str().c_str());
    }


void S3StoreLogSystem::LogVerboseMessage(const char* tag, Aws::Utils::Logging::LogLevel log_level, const std::string& message) {
    std::cout << "  * " << tag << ": " << message << std::endl;
}

void S3StoreLogSystem::Flush() { return; }