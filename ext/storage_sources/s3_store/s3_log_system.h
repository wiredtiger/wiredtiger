#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <atomic>

class S3LogSystem : public Aws::Utils::Logging::LogSystemInterface {

    public:
    S3LogSystem(WT_EXTENSION_API *wtApi, uint32_t wtVerbosityLevel);
    Aws::Utils::Logging::LogLevel
    GetLogLevel(void) const override
    {
        return logLevel;
    }
    void Log(
      Aws::Utils::Logging::LogLevel logLevel, const char *tag, const char *format, ...) override;
    void LogStream(Aws::Utils::Logging::LogLevel logLevel, const char *tag,
      const Aws::OStringStream &messageStream) override;
    void Flush() override;

    private:
    void LogVerboseMessage(
      const char *tag, Aws::Utils::Logging::LogLevel logLevel, const std::string &message) const;
    std::atomic<Aws::Utils::Logging::LogLevel> logLevel;
    WT_EXTENSION_API *wtApi;
};
