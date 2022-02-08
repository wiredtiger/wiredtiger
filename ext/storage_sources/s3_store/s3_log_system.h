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
        return awsLogLevel;
    }
    void Log(
      Aws::Utils::Logging::LogLevel logLevel, const char *tag, const char *format, ...) override;
    void LogStream(Aws::Utils::Logging::LogLevel logLevel, const char *tag,
      const Aws::OStringStream &messageStream) override;
    void Flush() override;

    private:
    void LogAwsMessage(const char *tag, const std::string &message) const;
    void LogVerboseMessage(int32_t verbosityLevel, const std::string &message);
    std::atomic<Aws::Utils::Logging::LogLevel> awsLogLevel;
    WT_EXTENSION_API *wtApi;
    int32_t wtVerbosityLevel;
};
// Mapping the desired WiredTiger extension verbosity level to a rough equivalent AWS
// SDK verbosity level.
static const std::map<int32_t, Aws::Utils::Logging::LogLevel> verbosityMapping = {
  {-3, Aws::Utils::Logging::LogLevel::Error}, {-2, Aws::Utils::Logging::LogLevel::Warn},
  {-1, Aws::Utils::Logging::LogLevel::Info}, {0, Aws::Utils::Logging::LogLevel::Info},
  {1, Aws::Utils::Logging::LogLevel::Debug}};
