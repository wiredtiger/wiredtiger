/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "scoped_session.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {
/* ScopedSession implementation */
ScopedSession::ScopedSession(WT_CONNECTION *conn)
{
    Reinit(conn);
}

ScopedSession::~ScopedSession()
{
    if (_session != nullptr) {
        testutil_check(_session->close(_session, nullptr));
        _session = nullptr;
    }
}

ScopedSession::ScopedSession(ScopedSession &&other)
{
    std::swap(_session, other._session);
}

/*
 * Implement move assignment by move constructing a temporary and swapping its internals with the
 * current session. This means that the currently held WT_SESSION will get destroyed as the
 * temporary falls out of the scope and we will steal the one that we're move assigning from.
 */
ScopedSession &
ScopedSession::operator=(ScopedSession &&other)
{
    ScopedSession tmp(std::move(other));
    std::swap(_session, tmp._session);
    return (*this);
}

void
ScopedSession::Reinit(WT_CONNECTION *connection)
{
    if (_session != nullptr) {
        testutil_check(_session->close(_session, nullptr));
        _session = nullptr;
    }
    if (connection != nullptr)
        testutil_check(connection->open_session(connection, nullptr, nullptr, &_session));
}

WT_SESSION &
ScopedSession::operator*()
{
    return (*_session);
}

WT_SESSION *
ScopedSession::operator->()
{
    return (_session);
}

WT_SESSION *
ScopedSession::Get()
{
    return (_session);
}

ScopedCursor
ScopedSession::OpenScopedCursor(const std::string &uri, const std::string &cfg)
{
    return (ScopedCursor(_session, uri, cfg));
}
} // namespace test_harness
