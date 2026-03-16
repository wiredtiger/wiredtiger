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
#include "test_util.h"

/*
 * testutil_disagg_storage_configuration --
 *     Set up disagg storage configuration.
 */
void
testutil_disagg_storage_configuration(TEST_OPTS *opts, const char *home, char *disagg_cfg,
  size_t disagg_cfg_size, char *ext_cfg, size_t ext_cfg_size)
{
    (void)home;
    char key_provider_ext_cfg[256];

    if (opts->disagg.is_enabled) {
        testutil_snprintf(ext_cfg, ext_cfg_size, TESTUTIL_ENV_CONFIG_DISAGG_EXT, opts->build_dir,
          opts->disagg.page_log, opts->disagg.page_log, opts->disagg.page_log_home, opts->delay_ms,
          opts->error_ms, opts->force_delay, opts->force_error, opts->disagg.page_log_map_size_mb,
          opts->disagg.page_log_verbose);

        if (opts->disagg.key_provider) {
            testutil_snprintf(key_provider_ext_cfg, sizeof(key_provider_ext_cfg),
              TESTUTIL_ENV_CONFIG_KEY_PROVIDER_EXT, opts->build_dir);
            testutil_strcat(
              ext_cfg, ext_cfg_size + sizeof(key_provider_ext_cfg), key_provider_ext_cfg);
        }

        testutil_snprintf(disagg_cfg, disagg_cfg_size, TESTUTIL_ENV_CONFIG_DISAGG,
          opts->disagg.mode, opts->disagg.page_log, opts->disagg.drain_threads,
          (opts->disagg.internal_page_delta ? "true" : "false"),
          (opts->disagg.leaf_page_delta ? "true" : "false"));
    } else {
        testutil_snprintf(ext_cfg, ext_cfg_size, "\"\"");
        testutil_assert(disagg_cfg_size > 0);
        disagg_cfg[0] = '\0';
    }
}

static void
preserve_copy_uri(WT_SESSION *session, const char *from_uri, const char *to_uri, int max_entries)
{
    WT_DECL_RET;
    WT_CURSOR *from, *to;
    WT_ITEM key, value;
    int entries;
    char new_config[256];

    testutil_check(session->open_cursor(session, from_uri, NULL, "raw", &from));
    testutil_snprintf(new_config, sizeof(new_config), "key_format=%s,value_format=%s",
      from->key_format, from->value_format);
    testutil_check(session->create(session, to_uri, new_config));
    testutil_check(session->open_cursor(session, to_uri, NULL, "raw", &to));
    entries = 0;
    while ((max_entries < 0 || entries < max_entries) && (ret = from->next(from)) == 0) {
        from->get_key(from, &key);
        from->get_value(from, &value);
        to->set_key(to, &key);
        to->set_value(to, &value);
        testutil_check(to->insert(to));
        ++entries;
    }
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);
    testutil_check(from->close(from));
    testutil_check(to->close(to));
}

/*
 * testutil_disagg_preserve --
 *     Save the components of disaggregated and layered tables to regular local tables. The ingest
 *     table, the stable table and the composite view of the layered table is saved, for layered
 *     tables found in the metadata. This is typically called after a failure has occurred.
 */
void
testutil_disagg_preserve(TEST_OPTS *opts, WT_CONNECTION *conn, int max_entries, uint64_t ts)
{
    WT_DECL_RET;
    WT_SESSION *session;
    WT_CURSOR *metacopy, *metacur;
    const char *home, *uri, *metavalue;
    char config_buf[256], from_uri[1024], to_uri[1024];

    (void)opts;
    home = conn->get_home(conn);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /*
     * First make sure we don't have preserve entries in the metadata table.
     */
    testutil_check(session->open_cursor(session, "metadata:", NULL, NULL, &metacur));
    while ((ret = metacur->next(metacur)) == 0) {
        metacur->get_key(metacur, &uri);
        testutil_assertfmt(
          strstr(uri, "_preserve.wt") == NULL, "%s: connection has already been preserved", uri);
    }
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);

    /*
     * Next, remove any preserve files left over from a previous run.
     */
    testutil_remove_match(home, "_preserve.wt");

    session->begin_transaction(session, NULL);
    to_uri[0] = '\0';
    testutil_check(__wt_strcat(to_uri, sizeof(to_uri), "file:WiredTiger.wt_preserve"));
    testutil_check(session->create(session, to_uri, "key_format=S,value_format=S"));
    testutil_check(session->open_cursor(session, to_uri, NULL, NULL, &metacopy));

    /*
     * Now, for each layered table in metadata, copy its layered component to a newly created
     * preserve table.
     */
    testutil_check(metacur->reset(metacur));
    while ((ret = metacur->next(metacur)) == 0) {
        /* Copy the metadata to a preserve file. */
        metacur->get_key(metacur, &uri);
        metacur->get_value(metacur, &metavalue);
        metacopy->set_key(metacopy, uri);
        metacopy->set_value(metacopy, metavalue);
        testutil_check(metacopy->insert(metacopy));

        if (strncmp(uri, "layered:", 8) == 0) {
            uri += 8;
            testutil_snprintf(from_uri, sizeof(from_uri), "file:%s.wt_ingest", uri);
            testutil_snprintf(to_uri, sizeof(to_uri), "file:%s.wt_ingest_preserve", uri);
            preserve_copy_uri(session, from_uri, to_uri, max_entries);

            testutil_snprintf(from_uri, sizeof(from_uri), "file:%s.wt_stable", uri);
            testutil_snprintf(to_uri, sizeof(to_uri), "file:%s.wt_stable_preserve", uri);
            preserve_copy_uri(session, from_uri, to_uri, max_entries);

            testutil_snprintf(from_uri, sizeof(from_uri), "layered:%s", uri);
            testutil_snprintf(to_uri, sizeof(to_uri), "file:%s.wt_layered_preserve", uri);
            preserve_copy_uri(session, from_uri, to_uri, max_entries);
        }
    }
    testutil_assert(ret == 0 || ret == WT_NOTFOUND);
    testutil_check(metacur->close(metacur));
    testutil_check(metacopy->reset(metacopy));

    testutil_snprintf(config_buf, sizeof(config_buf), "commit_timestamp=%" PRIx64, ts);
    session->commit_transaction(session, config_buf);

    /*
     * At the moment, the only way we have to guarantee that the preserve files are on disk is to
     * checkpoint.
     */
    testutil_snprintf(config_buf, sizeof(config_buf), "stable_timestamp=%" PRIx64, ts);
    conn->set_timestamp(conn, config_buf);
    testutil_check(session->checkpoint(session, NULL));

    testutil_check(session->close(session, NULL));
}
