/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "cursor_wrapper.h"
#include "../utils.h"


CursorWrapper::CursorWrapper(WT_SESSION *_session, std::string const& tableName)
    : _wtCursor(nullptr)
{
    utils::throwIfNonZero(_session->open_cursor(_session, tableName.c_str(), nullptr, nullptr, &_wtCursor));
}


CursorWrapper::~CursorWrapper()
{
    close();
}


void
CursorWrapper::setKey(const std::string& key)
{
    _wtCursor->set_key(_wtCursor, key.c_str());
}


void
CursorWrapper::setValue(const std::string &value)
{
    _wtCursor->set_value(_wtCursor, value.c_str());
}



std::string
CursorWrapper::getKey()
{
    char const *pKey = nullptr;
    utils::throwIfNonZero(_wtCursor->get_key(_wtCursor, &pKey));
    return { pKey };}


std::string
CursorWrapper::getValue()
{
    char const *pValue = nullptr;
    utils::throwIfNonZero(_wtCursor->get_value(_wtCursor, &pValue));
    return { pValue };
}


void
CursorWrapper::reset()
{
    utils::throwIfNonZero(_wtCursor->reset(_wtCursor));
}


void
CursorWrapper::close()
{
    _wtCursor->close(_wtCursor);;
}


void
CursorWrapper::search()
{
    utils::throwIfNonZero(_wtCursor->search(_wtCursor));
}


void
CursorWrapper::insert()
{
    _wtCursor->insert(_wtCursor);
}


int
CursorWrapper::next()
{
    return (_wtCursor->next(_wtCursor));
}


int
CursorWrapper::prev()
{
    return (_wtCursor->prev(_wtCursor));
}

