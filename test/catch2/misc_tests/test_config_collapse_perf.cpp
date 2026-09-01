/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../wrappers/mock_session.h"

/*
 * Micro-benchmark and equivalence harness for the config-assembly path in __create_file (BF-44421):
 * __wt_config_collapse followed by __wt_config_tiered_strip, run per table creation.
 */

/* Copied verbatim from the generated file_meta base in src/config/config_def.c. */
static const char *file_meta_base =
  "access_pattern_hint=none,allocation_size=4KB,app_metadata=,"
  "assert=(commit_timestamp=none,durable_timestamp=none,"
  "read_timestamp=none,write_timestamp=off),block_allocation=best,"
  "block_compressor=,block_manager=default,cache_resident=false,"
  "checkpoint=,checkpoint_backup_info=,checkpoint_lsn=,checksum=on,"
  "collator=,columns=,dictionary=0,disaggregated=(page_log=,"
  "storage_tier=none),encryption=(keyid=,name=),format=btree,"
  "huffman_key=,huffman_value=,id=,"
  "ignore_in_memory_cache_size=false,in_memory=false,"
  "internal_item_max=0,internal_key_max=0,"
  "internal_key_truncate=true,internal_page_max=4KB,key_format=u,"
  "key_gap=10,leaf_item_max=0,leaf_key_max=0,leaf_page_max=32KB,"
  "leaf_value_max=0,live_restore=(bitmap=,nbits=0),"
  "log=(enabled=true),memory_page_image_max=0,memory_page_max=5MB,"
  "os_cache_dirty_max=0,os_cache_max=0,prefix_compression=false,"
  "prefix_compression_min=4,readonly=false,split_deepen_min_child=0"
  ",split_deepen_per_child=0,split_pct=90,tiered_object=false,"
  "tiered_storage=(auth_token=,bucket=,bucket_prefix=,"
  "cache_directory=,local_retention=300,name=,object_target_size=0,"
  "shared=false),value_format=u,verbose=[],version=(major=0,"
  "minor=0),write_timestamp_usage=none";

/* The config string __layered_create_missing_ingest_table builds. */
static const char *ingest_config =
  "key_format=\"S\",value_format=\"S\","
  "in_memory=true,log=(enabled=false),"
  "disaggregated=(page_log=none,storage_source=none)";

/* The id/version string __create_file appends. */
static const char *id_config = "id=26594,version=(major=2,minor=0),checkpoint_lsn=";

/*
 * reference_collapse --
 *     The pre-optimization __wt_config_collapse implementation, kept here to byte-compare against.
 */
static int
reference_collapse(WT_SESSION_IMPL *session, const char **cfg, char **config_ret)
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM k, v;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;

    *config_ret = NULL;

    WT_RET(__wt_scr_alloc(session, 1024, &tmp));

    __wt_config_init(session, &cparser, cfg[0]);
    while ((ret = __wt_config_next(&cparser, &k, &v)) == 0) {
        if (k.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRING &&
          k.type != WT_CONFIG_ITEM::WT_CONFIG_ITEM_ID)
            WT_ERR_MSG(session, EINVAL, "Invalid configuration key found: '%s'", k.str);
        WT_ERR(__wti_config_get(session, cfg, &k, &v));
        if (k.type == WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &k);
        if (v.type == WT_CONFIG_ITEM::WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &v);
        WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s=%.*s,", (int)k.len, k.str, (int)v.len, v.str));
    }

    if (ret != WT_NOTFOUND)
        goto err;

    if (tmp->size != 0)
        --tmp->size;
    ret = __wt_strndup(session, tmp->data, tmp->size, config_ret);

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

static double
time_collapse(WT_SESSION_IMPL *session, int iterations, int reps)
{
    const char *cfg[] = {file_meta_base, ingest_config, id_config, nullptr};
    double best = 1e30;

    for (int rep = 0; rep < reps; ++rep) {
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            char *config_ret = nullptr;
            REQUIRE(__wt_config_collapse(session, cfg, &config_ret) == 0);
            __wt_free(session, config_ret);
        }
        auto stop = std::chrono::steady_clock::now();
        double usecs = std::chrono::duration<double, std::micro>(stop - start).count() / iterations;
        best = std::min(best, usecs);
    }
    return best;
}

static double
time_strip(WT_SESSION_IMPL *session, const char *fileconf, int iterations, int reps)
{
    const char *cfg[] = {file_meta_base, fileconf, nullptr};
    double best = 1e30;

    for (int rep = 0; rep < reps; ++rep) {
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < iterations; ++i) {
            const char *config_ret = nullptr;
            REQUIRE(__wt_config_tiered_strip(session, cfg, &config_ret) == 0);
            __wt_free(session, config_ret);
        }
        auto stop = std::chrono::steady_clock::now();
        double usecs = std::chrono::duration<double, std::micro>(stop - start).count() / iterations;
        best = std::min(best, usecs);
    }
    return best;
}

/*
 * Randomized equivalence check: build configuration stacks with duplicate keys, unknown keys,
 * nested structures and mixed value types, and require the optimized collapse to produce a
 * byte-identical string to the reference implementation.
 */
static void
fuzz_collapse_equivalence(WT_SESSION_IMPL *session, uint32_t seed, int cases)
{
    static const std::vector<std::string> keys = {"access_pattern_hint", "allocation_size",
      "app_metadata", "block_compressor", "checksum", "columns", "dictionary", "disaggregated",
      "in_memory", "key_format", "log", "memory_page_max", "prefix_compression", "split_pct",
      "tiered_storage", "value_format", "verbose", "version", "unknown_key_a", "unknown_key_b"};
    static const std::vector<std::string> values = {"", "0", "1", "true", "false", "none", "4KB",
      "90", "300", "5MB", "\"S\"", "\"u\"", "\"hello world\"", "(enabled=true)", "(enabled=false)",
      "(page_log=none,storage_source=none)", "(major=2,minor=0)",
      "(auth_token=,bucket=,shared=false)", "[]", "(bitmap=,nbits=0)"};

    std::mt19937 gen(seed);

    for (int tc = 0; tc < cases; ++tc) {
        /* 1-3 overriding strings. */
        size_t nstr = 1 + gen() % 3;
        std::vector<std::string> strings(nstr + 1);
        for (size_t s = 0; s <= nstr; ++s) {
            size_t nkeys = 1 + gen() % 12;
            std::string str;
            for (size_t i = 0; i < nkeys; ++i) {
                if (!str.empty())
                    str += ",";
                str += keys[gen() % keys.size()];
                str += "=";
                str += values[gen() % values.size()];
            }
            strings[s] = str;
        }

        std::vector<const char *> cfg;
        for (auto &s : strings)
            cfg.push_back(s.c_str());
        cfg.push_back(nullptr);

        char *got = nullptr, *want = nullptr;
        REQUIRE(reference_collapse(session, cfg.data(), &want) == 0);
        REQUIRE(__wt_config_collapse(session, cfg.data(), &got) == 0);
        INFO("case " << tc << " seed " << seed << " cfg[0]: " << strings[0]);
        REQUIRE(std::string(got) == std::string(want));
        __wt_free(session, got);
        __wt_free(session, want);
    }
}

TEST_CASE("Config collapse/strip per-create cost", "[config][collapse_perf]")
{
    auto mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();

    /* Equivalence with the reference implementation, including the workload's own stack. */
    {
        const char *cfg[] = {file_meta_base, ingest_config, id_config, nullptr};
        char *got = nullptr, *want = nullptr;
        REQUIRE(reference_collapse(session, cfg, &want) == 0);
        REQUIRE(__wt_config_collapse(session, cfg, &got) == 0);
        REQUIRE(std::string(got) == std::string(want));
        __wt_free(session, got);

        char *fileconf = want;

        /* Show what skipping the tiered strip changes in the persisted metadata. */
        const char *stripcfg[] = {file_meta_base, fileconf, nullptr};
        const char *stripped = nullptr;
        REQUIRE(__wt_config_tiered_strip(session, stripcfg, &stripped) == 0);
        std::string collapsed_str(fileconf), stripped_str(stripped);
        std::printf(
          "strip output == collapse output: %s\n", collapsed_str == stripped_str ? "YES" : "NO");
        if (collapsed_str != stripped_str) {
            std::printf(
              "--- collapse (%zu bytes) ---\n%s\n", collapsed_str.size(), collapsed_str.c_str());
            std::printf(
              "--- strip    (%zu bytes) ---\n%s\n", stripped_str.size(), stripped_str.c_str());
        }

        /*
         * Compare guard placements. Call-site guard: metadata is fileconf. In-function guard: the
         * strip call on [base, fileconf] becomes collapse([base, fileconf]). Show whether the two
         * produce the same bytes.
         */
        char *recollapsed = nullptr;
        REQUIRE(__wt_config_collapse(session, stripcfg, &recollapsed) == 0);
        std::printf("in-function guard output == call-site guard output: %s\n",
          std::string(recollapsed) == collapsed_str ? "YES" : "NO");
        __wt_free(session, recollapsed);

        /* Show the semantic trap: merge is additive across strings, collapse masks to cfg[0]. */
        {
            const char *partial[] = {"type=file", "app_metadata=(x=1),custom_key=5", nullptr};
            const char *merged = nullptr;
            char *collapsed = nullptr;
            REQUIRE(__wt_config_tiered_strip(session, partial, &merged) == 0);
            REQUIRE(__wt_config_collapse(session, partial, &collapsed) == 0);
            std::printf("partial cfg[0] stack:\n  merge   : %s\n  collapse: %s\n", merged,
              collapsed);
            __wt_free(session, merged);
            __wt_free(session, collapsed);
        }

        constexpr int iterations = 20000, reps = 3;
        double collapse_us = time_collapse(session, iterations, reps);
        double strip_us = time_strip(session, fileconf, iterations, reps);

        std::printf("iterations=%d reps=%d (best of reps)\n", iterations, reps);
        std::printf("collapse : %8.1f us/call\n", collapse_us);
        std::printf("strip    : %8.1f us/call\n", strip_us);
        std::printf(
          "combined : %8.1f us/call  (measured in-workload: ~273 us)\n", collapse_us + strip_us);
        std::printf("scaled to 50,029 tables: %.1f s\n", (collapse_us + strip_us) * 50029 / 1e6);

        __wt_free(session, fileconf);
        __wt_free(session, stripped);
    }

    fuzz_collapse_equivalence(session, 0xbf44421, 2000);
}
