/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*      All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#ifndef WIREDTIGER_EVENT_HANDLER_H
#define WIREDTIGER_EVENT_HANDLER_H

#include "wiredtiger.h"


class EventHandler {
    struct CustomEventHandler {
        WT_EVENT_HANDLER _wtEventHandler;
        EventHandler* _eventHandler;
    };

    public:
    EventHandler();
    virtual ~EventHandler() = default;
    WT_EVENT_HANDLER* getWtEventHandler() { return &_customEventHandler._wtEventHandler; };

    static int redirectHandleError(WT_EVENT_HANDLER* eventHandler, WT_SESSION *session, int error, const char *message);
    static int redirectHandleMessage(WT_EVENT_HANDLER* eventHandler, WT_SESSION *session, const char *message) ;
    static int redirectHandleProgress(WT_EVENT_HANDLER* eventHandler, WT_SESSION *session, const char *operation, uint64_t progress);
    static int redirectHandleClose(WT_EVENT_HANDLER* eventHandler, WT_SESSION *session, WT_CURSOR *cursor);

    protected:
    virtual int handleError(WT_SESSION *session, int error, const char *message);
    virtual int handleMessage(WT_SESSION *session, const char *message)  { return 0; };
    virtual int handleProgress(WT_SESSION *session, const char *operation, uint64_t progress) { return 0; };
    virtual int handleClose(WT_SESSION *session, WT_CURSOR *cursor) { return 0; };

    private:
    CustomEventHandler _customEventHandler;
};

#endif // WIREDTIGER_EVENT_HANDLER_H
