/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*      All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#include <iostream>
#include "event_handler.h"

EventHandler::EventHandler()
{
    _customEventHandler._wtEventHandler.handle_error    = &redirectHandleError;
    _customEventHandler._wtEventHandler.handle_message  = &redirectHandleMessage;
    _customEventHandler._wtEventHandler.handle_progress = &redirectHandleProgress;
    _customEventHandler._wtEventHandler.handle_close    = &redirectHandleClose;
    _customEventHandler._eventHandler = this;
}


int
EventHandler::redirectHandleError(WT_EVENT_HANDLER *eventHandler, WT_SESSION *session, int error, const char *message)
{
    auto* customEventHandler = reinterpret_cast<CustomEventHandler*>(eventHandler);
    return customEventHandler->_eventHandler->handleError(session, error, message);
}


int
EventHandler::redirectHandleMessage(
  WT_EVENT_HANDLER *eventHandler, WT_SESSION *session, const char *message)
{
    auto* customEventHandler = reinterpret_cast<CustomEventHandler*>(eventHandler);
    return customEventHandler->_eventHandler->handleMessage(session, message);
}


int
EventHandler::redirectHandleProgress(
  WT_EVENT_HANDLER *eventHandler, WT_SESSION *session, const char *operation, uint64_t progress)
{
    auto* customEventHandler = reinterpret_cast<CustomEventHandler*>(eventHandler);
    return customEventHandler->_eventHandler->handleProgress(session, operation, progress);
}


int
EventHandler::redirectHandleClose(
  WT_EVENT_HANDLER *eventHandler, WT_SESSION *session, WT_CURSOR *cursor)
{
    auto* customEventHandler = reinterpret_cast<CustomEventHandler*>(eventHandler);
    return customEventHandler->_eventHandler->handleClose(session, cursor);
}


int
EventHandler::handleError(WT_SESSION *session, int error, const char *message)
{
    std::cerr << "EventHandler::handleError: error = " << error << ", message = '" << message << "'" << std::endl;
    return 0;
}


int
EventHandler::handleMessage(WT_SESSION *session, const char *message)
{
    //std::cerr << "EventHandler::handleMessage: message = '" << message << "'" << std::endl;
    fprintf(stderr, "EventHandler::handleMessage: message = '%s'\n", message);
    return 0;
}


WT_EVENT_HANDLER *
EventHandler::getWtEventHandler()
{
    return &_customEventHandler._wtEventHandler;
}


int
EventHandler::handleProgress(WT_SESSION *session, const char *operation, uint64_t progress)
{
    return 0;
}


int
EventHandler::handleClose(WT_SESSION *session, WT_CURSOR *cursor)
{
    return 0;
}
