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
#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <atomic>

// Mapping the desired WiredTiger extension verbosity level to a rough equivalent AWS
// SDK verbosity level.
static const std::map<int32_t, Aws::Utils::Logging::LogLevel> verbosityMapping = {
  {-3, Aws::Utils::Logging::LogLevel::Error}, {-2, Aws::Utils::Logging::LogLevel::Warn},
  {-1, Aws::Utils::Logging::LogLevel::Info}, {0, Aws::Utils::Logging::LogLevel::Info},
  {1, Aws::Utils::Logging::LogLevel::Debug}};

// This class provides the S3 Store with a logger implementation that redirects the generated
// logs to WiredTiger's logging streams. This class implements the AWS LogSystemInterface
// to derive functions to incorporate the logging with WiredTiger's logging system. AWS SDK 
// also registers the same logger, and the best attempt is made to match the SDK's logging
// levels to WiredTiger's. 
class S3LogSystem : public Aws::Utils::Logging::LogSystemInterface {
    public:
    S3LogSystem(WT_EXTENSION_API *wtApi, uint32_t wtVerbosityLevel);
    Aws::Utils::Logging::LogLevel
    GetLogLevel(void) const override
    {
        return (_awsLogLevel);
    }

    // This function is inherited from AWS's LogSystemInterface. This function does a printf style
    // output to the output stream. This function is mostly unchanged from AWS' implementation. 
    void Log(
      Aws::Utils::Logging::LogLevel logLevel, const char *tag, const char *format, ...) override;

    // This function is inherited from AWS's LogSystemInterface. This function writes the log stream to the 
    // output stream, in this case WiredTiger's output stream. 
    void LogStream(Aws::Utils::Logging::LogLevel logLevel, const char *tag,
      const Aws::OStringStream &messageStream) override;

    // This function sends error messages to the WiredTiger's error level log stream. 
    void LogErrorMessage(const std::string &message) const;
    
    // This function sends error messages to the WiredTiger's debug level log stream. 
    void LogDebugMessage(const std::string &message) const;

    // This function sets the WiredTiger Extension's verbosity level and matches the AWS log levels to this. 
    void SetWtVerbosityLevel(int32_t wtVerbosityLevel);

    // This function is inherited from AWS LogSystemInterface and is not implemented. 
    void Flush() override;

    private:
    void LogAwsMessage(const char *tag, const std::string &message) const;
    void LogVerboseMessage(int32_t verbosityLevel, const std::string &message) const;
    std::atomic<Aws::Utils::Logging::LogLevel> _awsLogLevel;
    WT_EXTENSION_API *_wtApi;
    int32_t _wtVerbosityLevel;
};
