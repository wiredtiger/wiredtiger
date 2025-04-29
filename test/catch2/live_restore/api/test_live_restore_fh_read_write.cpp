/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Test the live restore __live_restore_fh_read, __live_restore_fh_write functionality.
 * [live_restore_fh_read_write].
 */

#include "../utils_live_restore.h"

using namespace utils;

void
init_file_handle(WT_SESSION *session, WTI_LIVE_RESTORE_FS *lr_fs, const char *file_name,
  int allocsize, int file_size, WTI_LIVE_RESTORE_FILE_HANDLE **lr_fhp)
{
    lr_fs->iface.fs_open_file((WT_FILE_SYSTEM *)lr_fs, session, file_name,
      WT_FS_OPEN_FILE_TYPE_DATA, WT_FS_OPEN_CREATE, (WT_FILE_HANDLE **)lr_fhp);

    WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh = *lr_fhp;
    lr_fh->allocsize = allocsize;
    lr_fh->nbits = file_size / allocsize;
    REQUIRE(__bit_alloc((WT_SESSION_IMPL *)session, lr_fh->nbits, &lr_fh->bitmap) == 0);
}

TEST_CASE("Live Restore fh_read fh_write", "[live_restore],[live_restore_fh_read_write]")
{
    auto file_name = "test_table.wt";
    int allocsize = 4, page_size = allocsize * 4, file_size = 132;
    auto dummy_char = '0';
    auto src_char = '1';
    auto write_char = '2';

    live_restore_test_env env;
    WT_SESSION *session = reinterpret_cast<WT_SESSION *>(env.session);
    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    WTI_LIVE_RESTORE_FS *lr_fs = env.lr_fs;
    WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh = nullptr;

    SECTION("Read/write when source exists")
    {
        create_file(env.source_file_path(file_name), file_size, src_char);
        // Create a dest file, pre-filled dummy chars in the dest file is used to verify we are not
        // touching them when their corresponding bits are set in the bitmap.
        create_file(env.dest_file_path(file_name), file_size, dummy_char);

        init_file_handle(
          session, lr_fs, env.dest_file_path(file_name).c_str(), allocsize, file_size, &lr_fh);
        char *write_buf = nullptr, *read_buf = nullptr;

        // No writes yet, reads go into src.
        REQUIRE(__wt_calloc(session_impl, 1, file_size + 1, &read_buf) == 0);
        auto data = std::string(page_size, src_char);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        /*
         * Background migration in progress, simulate this by calling a fh_write() to write src
         * chars to dest, where the first few pages are fully migrated and the last page is
         * partially migrated.
         */
        auto count = 13;
        auto background_write_len = allocsize * count;
        REQUIRE(background_write_len <= file_size);
        data = std::string(background_write_len, src_char);
        REQUIRE(__wt_calloc(session_impl, 1, background_write_len + 1, &write_buf) == 0);
        REQUIRE(__wt_snprintf(write_buf, background_write_len + 1, "%s", data.c_str()) == 0);
        REQUIRE(lr_fh->iface.fh_write(
                  (WT_FILE_HANDLE *)lr_fh, session, 0, background_write_len, write_buf) == 0);

        // The first floor(migration_page_count) are fully migrated and the data should have been
        // copied to the dest file.
        data = std::string(page_size, src_char);
        int offset = 0;
        for (; offset + page_size < background_write_len; offset += page_size) {
            REQUIRE(lr_fh->iface.fh_read(
                      (WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
            REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
            // Verify data has been written to the dest file.
            REQUIRE(lr_fh->destination->fh_read(
                      lr_fh->destination, session, offset, page_size, read_buf) == 0);
            REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        }

        // The last page is partially migrated, since there's no write on dest yet read will just
        // get src chars.
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
        data = std::string(page_size, src_char);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        // However in the dest file we should only see the first part of the page copied where the
        // second part should still be dummy chars.
        REQUIRE(lr_fh->destination->fh_read(
                  lr_fh->destination, session, offset, page_size, read_buf) == 0);
        auto migrated_size = background_write_len % page_size;
        data =
          std::string(migrated_size, src_char) + std::string(page_size - migrated_size, dummy_char);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Test write happens after migration, this should overwrite data in dest where src remains
        // unchanged.
        data = std::string(page_size, write_char);
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        REQUIRE(
          lr_fh->iface.fh_write((WT_FILE_HANDLE *)lr_fh, session, 0, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        REQUIRE(
          lr_fh->destination->fh_read(lr_fh->destination, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        data = std::string(page_size, src_char);
        REQUIRE(lr_fh->source->fh_read(lr_fh->source, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Test a write that partially exceeds the bitmap.
        data = std::string(page_size, write_char);
        offset = file_size / page_size * page_size;
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        REQUIRE(lr_fh->iface.fh_write(
                  (WT_FILE_HANDLE *)lr_fh, session, offset, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        REQUIRE(lr_fh->destination->fh_read(
                  lr_fh->destination, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Test a write that goes completely beyond the bitmap range.
        data = std::string(page_size, write_char);
        offset = (file_size / page_size + 5) * page_size;
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        REQUIRE(lr_fh->iface.fh_write(
                  (WT_FILE_HANDLE *)lr_fh, session, offset, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        REQUIRE(lr_fh->destination->fh_read(
                  lr_fh->destination, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Source should remain untouched during the whole test.
        data = std::string(file_size, src_char);
        REQUIRE(lr_fh->source->fh_read(lr_fh->source, session, 0, file_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), file_size));

        REQUIRE(lr_fh->iface.close((WT_FILE_HANDLE *)lr_fh, session) == 0);
        __wt_free(session_impl, read_buf);
        __wt_free(session_impl, write_buf);
    }

    SECTION("Read/write when source not exists")
    {
        testutil_remove(env.source_file_path(file_name).c_str());
        testutil_remove(env.dest_file_path(file_name).c_str());
        create_file(env.dest_file_path(file_name), file_size, dummy_char);

        init_file_handle(
          session, lr_fs, env.dest_file_path(file_name).c_str(), allocsize, file_size, &lr_fh);
        char *write_buf = NULL, *read_buf = NULL;

        auto data = std::string(page_size, write_char);
        REQUIRE(__wt_calloc(session_impl, 1, page_size + 1, &read_buf) == 0);
        REQUIRE(__wt_calloc(session_impl, 1, page_size + 1, &write_buf) == 0);
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        // Test written data can be read from fh_read().
        REQUIRE(
          lr_fh->iface.fh_write((WT_FILE_HANDLE *)lr_fh, session, 0, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        // Verify data has been written to the dest file.
        REQUIRE(
          lr_fh->destination->fh_read(lr_fh->destination, session, 0, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Test a write that partially exceeds the bitmap.
        data = std::string(page_size, write_char);
        auto offset = file_size / page_size * page_size;
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        REQUIRE(lr_fh->iface.fh_write(
                  (WT_FILE_HANDLE *)lr_fh, session, offset, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        REQUIRE(lr_fh->destination->fh_read(
                  lr_fh->destination, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Test a write that goes completely beyond the bitmap range.
        data = std::string(page_size, write_char);
        offset = (file_size / page_size + 5) * page_size;
        REQUIRE(__wt_snprintf(write_buf, page_size + 1, "%s", data.c_str()) == 0);
        REQUIRE(lr_fh->iface.fh_write(
                  (WT_FILE_HANDLE *)lr_fh, session, offset, page_size, write_buf) == 0);
        REQUIRE(
          lr_fh->iface.fh_read((WT_FILE_HANDLE *)lr_fh, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));
        REQUIRE(lr_fh->destination->fh_read(
                  lr_fh->destination, session, offset, page_size, read_buf) == 0);
        REQUIRE(WT_STRING_MATCH(read_buf, data.c_str(), page_size));

        // Source should remain untouched during the whole test.
        REQUIRE(lr_fh->source == nullptr);

        REQUIRE(lr_fh->iface.close((WT_FILE_HANDLE *)lr_fh, session) == 0);
        __wt_free(session_impl, read_buf);
        __wt_free(session_impl, write_buf);
    }
}
