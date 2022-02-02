#ifndef WT_CONNECTION_WRAPPER_H
#define WT_CONNECTION_WRAPPER_H

#include <memory>
#include "wt_internal.h"

class ConnectionWrapper {
    public:
        ~ConnectionWrapper();
        WT_CONNECTION_IMPL * getWtConnectionImpl() { return _connectionImpl; };
        WT_CONNECTION* getWtConnection() { return reinterpret_cast<WT_CONNECTION *>(_connectionImpl); };

        static std::shared_ptr<ConnectionWrapper> buildTestConnectionWrapper();

    private:
        explicit ConnectionWrapper(WT_CONNECTION_IMPL* connectionImpl);

        // ConnectionWrapper is implemented such that it owns, and is responsible for freeing, _connectionImpl
        WT_CONNECTION_IMPL* _connectionImpl;
};

#endif // WT_CONNECTION_WRAPPER_H
