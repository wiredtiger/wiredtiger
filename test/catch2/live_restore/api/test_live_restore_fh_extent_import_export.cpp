/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Test the live restore extent import and export functionality. [live_restore_extent_import_export]
 */

#include "../utils_live_restore.h"
#include "../../wrappers/mock_session.h"

using namespace utils;

// TEST_CASE("Live Restore extent import", "[live_restore],[live_restore_extent_import_export]")
// {
//     /*
//      * Note: this code runs once per SECTION so we're creating a brand new WT database for each
//      * section. If this gets slow we can make it static and manually clear the destination and
//      * source for each section.
//      */
//     live_restore_test_env env;
//     WT_SESSION_IMPL *session = env.session;
//     WT_SESSION *wt_session = (WT_SESSION *)session;
//     std::string file_name = "MY_FILE.txt";
//     std::string source_file = env.source_file_path(file_name);
//     std::string dest_file = env.dest_file_path(file_name);
//     SECTION(
//       "When opening a file instantiates a new destination file it will have a single hole which "
//       "matches it's size without importing a string")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 4096);
//         // This call creates the file in destination and a hole in that file the same size as the
//         // source file.
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         REQUIRE(extent_list_str(lr_fh) == "(0-4096)");
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test a single hole in the first 4KB of the file")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 4096);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 4096.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "0-4096") == 0);
//         REQUIRE(extent_list_str(lr_fh) == "(0-4096)");
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test a string import with numerous holes")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 40000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 40000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         // Extents are additive to compress the string size. I.e. the offset of extent 1 is the
//         // offset of extent 0 + the offset of extent 1.
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "0-4096;10000-10000;20001-1") == 0);
//         REQUIRE(extent_list_str(lr_fh) == "(0-4096), (10000-10000), (30001-1)");
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test that we cannot import a string with holes beyond the end of the source file")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         // The file ends at 999.
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "1000-1") == EINVAL);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test that we cannot import a string with a zero len extent")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "0-0") == EINVAL);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Invalid shape string test #1")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "-") == EINVAL);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Invalid shape string test #2")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "-1") == EINVAL);
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "1") == EINVAL);
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "string1") == EINVAL);
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, ";") == EINVAL);
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, ";;;") == EINVAL);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test an empty string")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         // An empty string or nullptr string marks the destination file as complete.
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, "") == 0);
//         REQUIRE(lr_fh->destination.complete == true);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Test a nullptr string")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         create_file(source_file.c_str(), 1000);
//         // We need to create a file in the destination from the get go otherwise we'll initialize
//         // it with one extent size 1000.
//         create_file(dest_file.c_str(), 0);
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         // An empty string or nullptr string marks the destination file as complete.
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, nullptr) == 0);
//         REQUIRE(lr_fh->destination.complete == true);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }

//     SECTION("Pass a string but unset the WT_CONN_LIVE_RESTORE_FS flag")
//     {
//         WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//         testutil_check(open_lr_fh(env, dest_file.c_str(), &lr_fh, WT_FS_OPEN_CREATE));
//         WT_CONNECTION_IMPL *conn = (WT_CONNECTION_IMPL *)session;
//         F_CLR(conn, WT_CONN_LIVE_RESTORE_FS);
//         // No file system means this function immediately returns 0.
//         REQUIRE(__wt_live_restore_fh_import_extents_from_string(
//                   session, (WT_FILE_HANDLE *)lr_fh, nullptr) == 0);
//         REQUIRE(lr_fh->iface.close(reinterpret_cast<WT_FILE_HANDLE *>(lr_fh), wt_session) == 0);
//     }
// }

// TEST_CASE("Live Restore extent export no live restore fs",
//   "[live_restore],[live_restore_extent_import_export]")
// {
//     std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
//     WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();
//     REQUIRE(__wt_live_restore_fh_extent_to_metadata(session, NULL, NULL) == WT_NOTFOUND);
// }

// WTI_LIVE_RESTORE_HOLE_NODE *
// __alloc_exent(WT_SESSION_IMPL *session, wt_off_t offset, size_t len)
// {
//     WTI_LIVE_RESTORE_HOLE_NODE *node;
//     REQUIRE(__wt_calloc_one(session, &node) == 0);

//     node->off = offset;
//     node->len = len;

//     return node;
// }

// TEST_CASE("Live Restore extent export", "[live_restore],[live_restore_extent_import_export]")
// {
//     std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
//     WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();
//     WT_CONNECTION_IMPL *conn = mock_session->get_mock_connection()->get_wt_connection_impl();
//     F_SET(conn, WT_CONN_LIVE_RESTORE_FS);

//     WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh;
//     REQUIRE(__wt_calloc_one(session, &lr_fh) == 0);
//     WT_ITEM string;
//     WT_CLEAR(string);
//     REQUIRE(__wt_buf_grow(session, &string, 1000) == 0);

//     SECTION("Pass a complete live restore file handle")
//     {
//         WT_UNUSED(string);
//         lr_fh->destination.complete = true;
//         REQUIRE(__wt_live_restore_fh_extent_to_metadata(session, (WT_FILE_HANDLE *)lr_fh, NULL)
//         ==
//           WT_NOTFOUND);
//     }

//     SECTION("Test a file handle with no extents")
//     {
//         REQUIRE(
//           __wt_live_restore_fh_extent_to_metadata(session, (WT_FILE_HANDLE *)lr_fh, &string) ==
//           0);
//         REQUIRE(std::string((char *)string.data) == ",live_restore=");
//     }

//     SECTION("Test a file handle with one extent")
//     {
//         WTI_LIVE_RESTORE_HOLE_NODE *node;
//         node = __alloc_exent(session, 0, 4096);
//         lr_fh->destination.hole_list_head = node;
//         REQUIRE(
//           __wt_live_restore_fh_extent_to_metadata(session, (WT_FILE_HANDLE *)lr_fh, &string) ==
//           0);
//         REQUIRE(std::string((char *)string.data) == ",live_restore=0-4096");
//         __wt_free(session, lr_fh->destination.hole_list_head);
//     }

//     SECTION("Test a file handle with many extents")
//     {
//         WTI_LIVE_RESTORE_HOLE_NODE *node;
//         node = __alloc_exent(session, 0, 4096);
//         lr_fh->destination.hole_list_head = node;
//         lr_fh->destination.hole_list_head->next = __alloc_exent(session, 4096, 4096);
//         lr_fh->destination.hole_list_head->next->next = __alloc_exent(session, 8192, 10);
//         lr_fh->destination.hole_list_head->next->next->next = __alloc_exent(session, 100000, 10);
//         REQUIRE(extent_list_in_order(lr_fh));
//         REQUIRE(
//           __wt_live_restore_fh_extent_to_metadata(session, (WT_FILE_HANDLE *)lr_fh, &string) ==
//           0);
//         REQUIRE(
//           std::string((char *)string.data) == ",live_restore=0-4096;4096-4096;4096-10;91808-10");
//         __wt_free(session, lr_fh->destination.hole_list_head->next->next->next);
//         __wt_free(session, lr_fh->destination.hole_list_head->next->next);
//         __wt_free(session, lr_fh->destination.hole_list_head->next);
//         __wt_free(session, lr_fh->destination.hole_list_head);
//     }

//     __wt_free(session, lr_fh);
//     __wt_buf_free(session, &string);
// }
