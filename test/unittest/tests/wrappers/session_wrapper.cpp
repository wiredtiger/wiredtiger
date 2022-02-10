#include "session_wrapper.h"
#include <utility>
#include "connection_wrapper.h"
#include "error_handler.h"

SessionWrapper::SessionWrapper(WT_SESSION_IMPL* session,
                               std::shared_ptr<ConnectionWrapper> connectionWrapper)
  : _sessionImpl(session),
    _connectionWrapper(std::move(connectionWrapper))
{
}


SessionWrapper::~SessionWrapper() {
    __wt_free(nullptr, _sessionImpl);
}


std::shared_ptr<SessionWrapper> SessionWrapper::buildTestSessionWrapper() {
    auto connectionWrapper = ConnectionWrapper::buildTestConnectionWrapper();

    WT_SESSION_IMPL* sessionImpl = nullptr;
    ErrorHandler::throwIfNonZero(__wt_calloc(nullptr, 1, sizeof(WT_SESSION_IMPL), &sessionImpl));
    sessionImpl->iface.connection = connectionWrapper->getWtConnection();

    // Construct a SessionWrapper object that will now own sessionImpl & connectionWrapper.
    return std::shared_ptr<SessionWrapper>(new SessionWrapper(sessionImpl, connectionWrapper));
}

