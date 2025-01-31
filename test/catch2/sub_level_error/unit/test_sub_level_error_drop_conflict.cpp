/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <thread>
#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../../wrappers/connection_wrapper.h"
#include "../utils_sub_level_error.h"
#include "../../../utility/test_util.h"

/*
 * [sub_level_error_drop_conflict]: test_sub_level_error_drop_conflict.cpp
 * Tests the drop workflows that lead to EBUSY errors, and ensure that the correct sub level error
 * codes and messages are stored.
 */

#define URI "table:test_drop_conflict"
#define CONFLICT_BACKUP_MSG "the table is currently performing backup and cannot be dropped"
#define CONFLICT_DHANDLE_MSG "another thread is currently holding the data handle of the table"
#define CONFLICT_CHECKPOINT_LOCK_MSG "another thread is currently holding the checkpoint lock"
#define CONFLICT_SCHEMA_LOCK_MSG "another thread is currently holding the schema lock"
#define CONFLICT_TABLE_LOCK_MSG "another thread is currently holding the table lock"

static bool is_locked;
static bool can_unlock;

/*
 * prepare_session_and_error --
 *     Prepare a session and error_info struct to be used by the drop conflict tests.
 */
void
prepare_session_and_error(
  connection_wrapper *conn_wrapper, WT_SESSION **session, WT_ERROR_INFO **err_info)
{
    WT_CONNECTION *conn = conn_wrapper->get_wt_connection();
    REQUIRE(conn->open_session(conn, NULL, NULL, session) == 0);
    *err_info = &(((WT_SESSION_IMPL *)(*session))->err_info);
}

/*
 * thread_function_drop_no_wait --
 *     This function is designed to be used as a thread function, and drops a table without waiting.
 */
void
thread_function_drop_no_wait(WT_SESSION *session)
{
    session->drop(session, URI, "lock_wait=0");
}

/*
 * thread_function_hold_spinlock --
 *     This function is designed to be used as a thread function, and holds a lock for 1 second.
 */
void
thread_function_hold_spinlock(WT_SESSION *session, WT_SPINLOCK *spinlock)
{
    __wt_spin_lock((WT_SESSION_IMPL *)session, spinlock);
    is_locked = true;
    while (!can_unlock)
        continue;
    __wt_spin_unlock((WT_SESSION_IMPL *)session, spinlock);
}

/*
 * thread_function_hold_rwlock --
 *     This function is designed to be used as a thread function, and holds a lock for 1 second.
 */
void
thread_function_hold_rwlock(WT_SESSION *session, WT_RWLOCK *rwlock)
{
    __wt_writelock((WT_SESSION_IMPL *)session, rwlock);
    is_locked = true;
    while (!can_unlock)
        continue;
    __wt_writeunlock((WT_SESSION_IMPL *)session, rwlock);
}

/*
 * This test case covers EBUSY errors resulting from drop while cursors are still open on the table.
 */
TEST_CASE("Test WT_CONFLICT_BACKUP and WT_CONFLICT_DHANDLE", "[sub_level_error_drop_conflict]")
{
    std::string config = "key_format=S,value_format=S";
    WT_SESSION *session = NULL;
    WT_ERROR_INFO *err_info = NULL;

    SECTION("Test WT_CONFLICT_BACKUP")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info);
        REQUIRE(session->create(session, URI, config.c_str()) == 0);

        /* Open a backup cursor, then attempt to drop the table. */
        WT_CURSOR *backup_cursor;
        REQUIRE(session->open_cursor(session, "backup:", NULL, NULL, &backup_cursor) == 0);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_BACKUP, CONFLICT_BACKUP_MSG);
    }

    /* This section gives us coverage in __drop_file. */
    SECTION("Test WT_CONFLICT_DHANDLE with simple table")
    {
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info);
        REQUIRE(session->create(session, URI, config.c_str()) == 0);

        /* Open a cursor on a table, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, URI, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE, CONFLICT_DHANDLE_MSG);
    }

    /* This section gives us coverage in __drop_table. */
    SECTION("Test WT_CONFLICT_DHANDLE with columns")
    {
        config += ",columns=(col1,col2)";
        connection_wrapper conn_wrapper = connection_wrapper(".", "create");
        prepare_session_and_error(&conn_wrapper, &session, &err_info);
        REQUIRE(session->create(session, URI, config.c_str()) == 0);

        /* Open a cursor on a table with columns, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, URI, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE, CONFLICT_DHANDLE_MSG);
    }

    /*
     * This section gives us coverage in __drop_tiered. The dir_store extension is only supported
     * for POSIX systems, so skip this section on Windows.
     */
#ifndef _WIN32
    SECTION("Test WT_CONFLICT_DHANDLE with tiered storage")
    {
        /* Set up the connection and session to use tiered storage. */
        const char *home = "WT_TEST";
        testutil_system("rm -rf %s && mkdir %s && mkdir %s/%s", home, home, home, "bucket");
        connection_wrapper conn_wrapper = connection_wrapper(home,
          "create,tiered_storage=(bucket=bucket,bucket_prefix=pfx-,name=dir_store),extensions=(./"
          "ext/storage_sources/dir_store/libwiredtiger_dir_store.so)");

        prepare_session_and_error(&conn_wrapper, &session, &err_info);
        REQUIRE(session->create(session, URI, config.c_str()) == 0);

        /* Open a cursor on a table that uses tiered storage, then attempt to drop the table. */
        WT_CURSOR *cursor;
        REQUIRE(session->open_cursor(session, URI, NULL, NULL, &cursor) == 0);
        REQUIRE(session->drop(session, URI, NULL) == EBUSY);
        utils::check_error_info(err_info, EBUSY, WT_CONFLICT_DHANDLE, CONFLICT_DHANDLE_MSG);
    }
#endif
}

/*
 * This test case covers EBUSY errors resulting from drop while a lock is held by another thread.
 *
 * We need different threads holding the lock versus performing the drop (as opposed to having
 * another session take the lock within the same thread). This is because the Windows implementation
 * of __wt_spin_lock/__wt_try_spin_lock will still take the lock if it has already been taken by the
 * same thread, resulting in a successful (no conflicts) drop.
 */
TEST_CASE("Test conflicts with checkpoint/schema/table locks", "[sub_level_error_drop_conflict]")
{
    std::string config = "key_format=S,value_format=S";
    WT_SESSION *session_a = NULL;
    WT_SESSION *session_b = NULL;
    WT_ERROR_INFO *err_info_a = NULL;
    WT_ERROR_INFO *err_info_b = NULL;
    is_locked = false;
    can_unlock = false;

    connection_wrapper conn_wrapper = connection_wrapper(".", "create");
    prepare_session_and_error(&conn_wrapper, &session_a, &err_info_a);
    prepare_session_and_error(&conn_wrapper, &session_b, &err_info_b);
    REQUIRE(session_a->create(session_a, URI, config.c_str()) == 0);

    std::thread lock_thread;
    int sub_level_err;
    const char *err_msg;

    SECTION("Test CONFLICT_CHECKPOINT_LOCK")
    {
        WT_SPINLOCK *checkpoint_lock = &S2C((WT_SESSION_IMPL *)session_a)->checkpoint_lock;
        lock_thread =
          std::thread([&]() { thread_function_hold_spinlock(session_b, checkpoint_lock); });
        sub_level_err = WT_CONFLICT_CHECKPOINT_LOCK;
        err_msg = CONFLICT_CHECKPOINT_LOCK_MSG;
    }

    SECTION("Test CONFLICT_SCHEMA_LOCK")
    {
        WT_SPINLOCK *schema_lock = &S2C((WT_SESSION_IMPL *)session_a)->schema_lock;
        lock_thread = std::thread([&]() { thread_function_hold_spinlock(session_b, schema_lock); });
        sub_level_err = WT_CONFLICT_SCHEMA_LOCK;
        err_msg = CONFLICT_SCHEMA_LOCK_MSG;
    }

    SECTION("Test CONFLICT_TABLE_LOCK")
    {
        WT_RWLOCK *table_lock = &S2C((WT_SESSION_IMPL *)session_a)->table_lock;
        lock_thread = std::thread([&]() { thread_function_hold_rwlock(session_b, table_lock); });
        sub_level_err = WT_CONFLICT_TABLE_LOCK;
        err_msg = CONFLICT_TABLE_LOCK_MSG;
    }

    /* Wait until the lock has been acquired before trying to drop. */
    while (!is_locked)
        continue;

    /* Attempt to drop the table while another thread holds a checkpoint, schema or table lock. */
    REQUIRE(session_a->drop(session_a, URI, "lock_wait=0") == EBUSY);
    can_unlock = true;
    lock_thread.join();

    utils::check_error_info(err_info_a, EBUSY, sub_level_err, err_msg);
    utils::check_error_info(err_info_b, 0, WT_NONE, WT_ERROR_INFO_EMPTY);
}
