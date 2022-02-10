#ifndef WT_SESSION_H
#define WT_SESSION_H

#include <memory>
#include "wt_internal.h"
#include "connection_wrapper.h"

class SessionWrapper {
    public:
    ~SessionWrapper();
    WT_SESSION_IMPL *
    getWtSessionImpl()
    {
        return _sessionImpl;
    };

    static std::shared_ptr<SessionWrapper> buildTestSessionWrapper();

    private:
    explicit SessionWrapper(
      WT_SESSION_IMPL *sessionImpl, std::shared_ptr<ConnectionWrapper> connectionWrapper = nullptr);

    // SessionWrapper is implemented such that it owns, and is responsible for freeing, _sessionImpl
    WT_SESSION_IMPL *_sessionImpl;
    std::shared_ptr<ConnectionWrapper> _connectionWrapper;
};

#endif // WT_SESSION_H
