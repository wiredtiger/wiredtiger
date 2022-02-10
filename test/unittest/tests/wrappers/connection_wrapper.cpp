#include "connection_wrapper.h"
#include "error_handler.h"

ConnectionWrapper::ConnectionWrapper(WT_CONNECTION_IMPL *connectionImpl)
    : _connectionImpl(connectionImpl)
{
}

ConnectionWrapper::~ConnectionWrapper()
{
    __wt_free(nullptr, _connectionImpl);
}

std::shared_ptr<ConnectionWrapper>
ConnectionWrapper::buildTestConnectionWrapper()
{
    WT_CONNECTION_IMPL *connectionImpl = nullptr;
    ErrorHandler::throwIfNonZero(
      __wt_calloc(nullptr, 1, sizeof(WT_CONNECTION_IMPL), &connectionImpl));
    // Construct a Session object that will now own session.
    return std::shared_ptr<ConnectionWrapper>(new ConnectionWrapper(connectionImpl));
}
