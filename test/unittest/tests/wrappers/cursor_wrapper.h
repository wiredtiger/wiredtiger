/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef WIREDTIGER_CURSOR_WRAPPER_H
#define WIREDTIGER_CURSOR_WRAPPER_H

#include <string>
#include "wiredtiger.h"


class CursorWrapper {
    public:
    CursorWrapper(WT_SESSION* _session, std::string const& tableName);
    ~CursorWrapper();
    void setKey(std::string const& key);
    void setValue(std::string const& value);
    [[nodiscard]] std::string getValue();
    void insert();
    int next();
    void reset();
    void close();
    void search();

    WT_CURSOR* getWtCursor() { return _wtCursor; };

    private:
    WT_CURSOR* _wtCursor;
};

#endif // WIREDTIGER_CURSOR_WRAPPER_H
