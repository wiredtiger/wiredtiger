#include <wiredtiger.h>
#include <wiredtiger_ext.h>

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogLevel.h>

#include <atomic>

class S3StoreLogSystem : public Aws::Utils::Logging::LogSystemInterface {

    public:
    explicit S3StoreLogSystem(WT_EXTENSION_API* wtApi, uint32_t awsVerbose);
    ~S3StoreLogSystem() = default;
    Aws::Utils::Logging::LogLevel GetLogLevel(void) const override {return log_level; }
    void Log(Aws::Utils::Logging::LogLevel logLevel, const char* tag, const char* format, ...) override;
    void LogStream(Aws::Utils::Logging::LogLevel logLevel, const char* tag, const Aws::OStringStream &messageStream) override;
    void Flush() override;

    private:
    void LogVerboseMessage(const char* tag, Aws::Utils::Logging::LogLevel log_level,const std::string& message);
    std::atomic<Aws::Utils::Logging::LogLevel> log_level;
    WT_EXTENSION_API* wt_api; 
};
