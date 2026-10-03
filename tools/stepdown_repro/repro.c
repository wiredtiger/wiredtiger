/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "trace.h"

#include <pthread.h>
#include <sys/stat.h>

#define CHECK(call)                                                                       \
    do {                                                                                  \
        int check_ret = (call);                                                            \
        if (check_ret != 0) {                                                              \
            fprintf(stderr, "%s: %s\n", #call, wiredtiger_strerror(check_ret));             \
            exit(1);                                                                      \
        }                                                                                 \
    } while (0)

static const char victim_base[] = "0000552900.00/opqrstuvwxyzabcd";
static const char victim_layered[] = "0000552900.00/opqrstuvwxyza";
static const char neighbor_base[] = "0000552900.01/opqrstuvwxyzabcd";
static const char neighbor_layered[] = "0000552900.03/opqrstuvwxyza";
static WT_CONNECTION *connection;
static WT_SESSION *session;
static WT_CURSOR *base, *layered;
static const char *mode;
static const char *repro_home;

/*
 * dump_view --
 *     Save every key and value without truncation for independent validation.
 */
static void
dump_view(const char *stage, const char *name, WT_CURSOR *cursor)
{
    char path[4096];
    WT_ITEM key, val;
    int ret;

    snprintf(path, sizeof(path), "%s/%s-%s.tsv", repro_home, stage, name);
    FILE *out = fopen(path, "w");
    if (out == NULL) {
        perror(path);
        exit(1);
    }
    CHECK(cursor->reset(cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        CHECK(cursor->get_key(cursor, &key));
        CHECK(cursor->get_value(cursor, &val));
        for (size_t i = 0; i < key.size; ++i)
            fprintf(out, "%02x", ((const uint8_t *)key.data)[i]);
        fputc('\t', out);
        for (size_t i = 0; i < val.size; ++i)
            fprintf(out, "%02x", ((const uint8_t *)val.data)[i]);
        fputc('\n', out);
    }
    if (ret != WT_NOTFOUND)
        CHECK(ret);
    CHECK(cursor->reset(cursor));
    bool failed = ferror(out) != 0;
    if (fclose(out) != 0 || failed) {
        fprintf(stderr, "%s: dump write failed\n", path);
        exit(1);
    }
}

static void
timestamp(uint64_t oldest, uint64_t stable)
{
    char config[128];
    snprintf(config, sizeof(config), "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64,
      oldest, stable);
    CHECK(connection->set_timestamp(connection, config));
}

static void
commit(uint64_t ts)
{
    char config[64];
    snprintf(config, sizeof(config), "commit_timestamp=%" PRIx64, ts);
    CHECK(session->commit_transaction(session, config));
    CHECK(base->reset(base));
    CHECK(layered->reset(layered));
}

static WT_ITEM
value(char *buffer, size_t size)
{
    static const char alphabet[] = "LMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ";
    memcpy(buffer, "0000552900/", 11);
    for (size_t i = 11; i < size; ++i)
        buffer[i] = alphabet[(i - 11) % 26];
    WT_ITEM item = {.data = buffer, .size = size};
    return (item);
}

static void
put(WT_CURSOR *cursor, const char *key, WT_ITEM *val)
{
    WT_ITEM k = {.data = key, .size = strlen(key)};
    cursor->set_key(cursor, &k);
    cursor->set_value(cursor, val);
    CHECK(cursor->insert(cursor));
}

static void
open_cursors(void)
{
    CHECK(connection->open_session(connection, NULL, NULL, &session));
    CHECK(session->open_cursor(session, "table:T00001", NULL, NULL, &base));
    CHECK(session->open_cursor(session, "layered:T00002", NULL, NULL, &layered));
}

static void
close_cursors(void)
{
    CHECK(base->close(base));
    CHECK(layered->close(layered));
    CHECK(session->close(session, NULL));
}

static void *
checkpoint(void *arg)
{
    WT_SESSION *s;
    (void)arg;
    CHECK(connection->open_session(connection, NULL, NULL, &s));
    CHECK(s->checkpoint(s, NULL));
    CHECK(s->close(s, NULL));
    return (NULL);
}

static int
lookup(WT_CURSOR *cursor, const char *key)
{
    WT_ITEM k = {.data = key, .size = strlen(key)};
    cursor->set_key(cursor, &k);
    int ret = cursor->search(cursor);
    CHECK(cursor->reset(cursor));
    if (ret != WT_NOTFOUND)
        CHECK(ret);
    return (ret);
}

static void
compare(const char *stage)
{
    repro_stage(stage);
    CHECK(session->begin_transaction(session, NULL));
    int b = lookup(base, victim_base);
    int l = lookup(layered, victim_layered);
    printf("%s base=%d layered=%d\n", stage, b, l);
    repro_note((WT_SESSION_IMPL *)session, "logical-base", NULL, (uint64_t)(int64_t)b);
    repro_note((WT_SESSION_IMPL *)session, "logical-layered", NULL, (uint64_t)(int64_t)l);
    dump_view(stage, "base", base);
    dump_view(stage, "layered", layered);
    CHECK(session->rollback_transaction(session, NULL));
}

/*
 * dump_checkpoint --
 *     Read the published stable image independently of the layered merge.
 */
static void
dump_checkpoint(const char *stage)
{
    WT_CURSOR *cursor;

    if (strcmp(stage, "persisted") == 0) {
        cursor = ((WTI_CURSOR_LAYERED *)layered)->stable_cursor;
        assert(cursor != NULL);
        dump_view(stage, "stable-checkpoint", cursor);
        CHECK(layered->reset(layered));
        return;
    }
    CHECK(session->open_cursor(session, "file:T00002.wt_stable", NULL,
      "checkpoint=WiredTigerCheckpoint", &cursor));
    dump_view(stage, "stable-checkpoint", cursor);
    CHECK(cursor->close(cursor));
}

static int
ordinary_evict(void)
{
    WT_SESSION_IMPL *evict;
    WT_CURSOR *c;
    WT_REF *ref;
    WT_DECL_RET;
    CHECK(__wt_open_internal_session((WT_CONNECTION_IMPL *)connection, "repro-eviction", false,
      WT_SESSION_EVICTION, 0, &evict));
    CHECK(evict->iface.open_cursor(&evict->iface, "file:T00002.wt_stable", NULL, NULL, &c));
    WT_ITEM k = {.data = victim_layered, .size = sizeof(victim_layered) - 1};
    WT_DATA_HANDLE *dhandle = ((WT_CURSOR_BTREE *)c)->dhandle;
    WT_WITH_DHANDLE(evict, dhandle,
      WT_WITH_PAGE_INDEX(evict,
        ret = __wt_row_search((WT_CURSOR_BTREE *)c, &k, false, NULL, false, NULL)));
    CHECK(ret);
    ref = ((WT_CURSOR_BTREE *)c)->ref;
    WT_WITH_DHANDLE(evict, dhandle, repro_note(evict, "driver-eviction-pin", ref, 0));
    WT_ENTER_GENERATION(evict, WT_GEN_SPLIT);
    CHECK(c->reset(c));
    assert(WT_REF_GET_STATE(ref) == WT_REF_MEM);
    WT_WITH_DHANDLE(evict, dhandle, {
        if (!WT_REF_CAS_STATE(evict, ref, WT_REF_MEM, WT_REF_LOCKED))
            ret = EBUSY;
        else {
            uint32_t flags = strcmp(mode, "urgent") == 0 ? WT_EVICT_CALL_URGENT : 0;
            repro_note(evict, "driver-ordinary-eviction", ref, flags);
            ret = __wt_evict(evict, ref, WT_REF_MEM, flags);
            repro_note(evict, "driver-eviction-end", NULL, (uint64_t)(int64_t)ret);
        }
    });
    WT_LEAVE_GENERATION(evict, WT_GEN_SPLIT);
    CHECK(c->close(c));
    CHECK(evict->iface.close(&evict->iface, NULL));
    return (ret);
}

static void
pickup(void)
{
    WT_PAGE_LOG *pl;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args;
    char config[1024];
    memset(&args, 0, sizeof(args));
    CHECK(connection->get_page_log(connection, "palite", &pl));
    CHECK(pl->pl_get_complete_checkpoint(pl, session, &args));
    snprintf(config, sizeof(config), "disaggregated=(checkpoint_meta=\"%.*s\")",
      (int)args.checkpoint_metadata.size, (const char *)args.checkpoint_metadata.data);
    CHECK(connection->reconfigure(connection, config));
    free(args.checkpoint_metadata.mem);
    CHECK(pl->terminate(pl, session));
}

int
main(int argc, char **argv)
{
    char config[4096], bulk_buf[1442], neighbor_buf[177];
    pthread_t thread;
    if (argc != 4) {
        fprintf(stderr, "usage: %s HOME BUILD fail|no-fail|mirror-off|no-skip|urgent|ci-random\n",
          argv[0]);
        return (1);
    }
    mode = argv[3];
    if (strcmp(mode, "fail") != 0 && strcmp(mode, "no-fail") != 0 &&
      strcmp(mode, "mirror-off") != 0 && strcmp(mode, "no-skip") != 0 &&
      strcmp(mode, "urgent") != 0 && strcmp(mode, "ci-random") != 0) {
        fprintf(stderr, "unknown mode: %s\n", mode);
        return (1);
    }
    setvbuf(stdout, NULL, _IONBF, 0);
    const char *home = argv[1], *build = argv[2];
    repro_home = home;
    const char *mirror = strcmp(mode, "mirror-off") == 0 ? "false" : "true";
    snprintf(config, sizeof(config),
      "create,cache_size=3GB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,"
      "page_delta=(leaf_page_delta=true,internal_page_delta=true),"
      "disaggregated=(role=leader,page_log=palite,stepdown_write_mirroring=%s),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"]", mirror, build);
    repro_trace_open(home, mode);
    CHECK(wiredtiger_open(home, NULL, config, &connection));
    CHECK(connection->open_session(connection, NULL, NULL, &session));
    timestamp(1, 1);
    CHECK(session->create(session, "table:T00001",
      "key_format=u,value_format=u,log=(enabled=false),leaf_page_max=12KB"));
    CHECK(session->create(session, "layered:T00002",
      "key_format=u,value_format=u,allocation_size=512,leaf_page_max=512,leaf_value_max=5120,"
      "internal_page_max=4096,prefix_compression=true,prefix_compression_min=3,dictionary=260,"
      "memory_page_max=5MB,split_pct=67"));
    CHECK(session->close(session, NULL));
    open_cursors();
    repro_stage("seed");
    WT_ITEM bulk = value(bulk_buf, sizeof(bulk_buf));
    WT_ITEM neighbor = value(neighbor_buf, sizeof(neighbor_buf));
    printf("bulk_hash=%" PRIu64 " neighbor_hash=%" PRIu64 "\n",
      __wt_hash_city64(bulk.data, bulk.size), __wt_hash_city64(neighbor.data, neighbor.size));
    CHECK(session->begin_transaction(session, NULL));
    put(base, victim_base, &bulk);
    put(layered, victim_layered, &bulk);
    put(base, "0000552898.00/opqrstuvwxyzabcd", &neighbor);
    put(layered, "0000552898.00/opqrstuvwxyza", &neighbor);
    put(base, "0000552901.00/opqrstuv", &neighbor);
    put(layered, "0000552901.00/opqrstuvwxyza", &neighbor);
    commit(3365380);
    timestamp(5350300, 5350300);
    CHECK(session->checkpoint(session, NULL));
    close_cursors();
    CHECK(connection->reconfigure(connection, "disaggregated=(role=follower)"));
    CHECK(connection->close(connection, NULL));
    snprintf(config, sizeof(config),
      "cache_size=3GB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,"
      "page_delta=(leaf_page_delta=true,internal_page_delta=true),"
      "disaggregated=(role=follower,page_log=palite,stepdown_write_mirroring=%s),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"]", mirror, build);
    CHECK(wiredtiger_open(home, NULL, config, &connection));
    open_cursors();
    pickup();
    timestamp(5350300, 5350300);
    CHECK(connection->reconfigure(connection, "disaggregated=(role=leader)"));
    CHECK(session->checkpoint(session, NULL));
    compare("reopen-leader");

    CHECK(session->begin_transaction(session, NULL));
    put(base, "0000552901.00/opqrstuv", &bulk);
    put(layered, "0000552901.00/opqrstuvwxyza", &bulk);
    commit(5775000);
    timestamp(5775189, 5775189);
    repro_stage("periodic");
    repro_checkpoint_gate(1);
    CHECK(pthread_create(&thread, NULL, checkpoint, NULL));
    repro_checkpoint_wait();
    repro_stage("delete");
    CHECK(session->begin_transaction(session, "read_timestamp=5a4f30"));
    WT_ITEM kb = {.data = victim_base, .size = sizeof(victim_base) - 1};
    WT_ITEM kl = {.data = victim_layered, .size = sizeof(victim_layered) - 1};
    base->set_key(base, &kb);
    CHECK(base->remove(base));
    layered->set_key(layered, &kl);
    CHECK(layered->remove(layered));
    commit(5918559);
    repro_checkpoint_release();
    CHECK(pthread_join(thread, NULL));

    repro_stage("pre-boundary-eviction");
    timestamp(6078002, 6078002);
    repro_failure_arm();
    if (strcmp(mode, "ci-random") == 0)
        CHECK(connection->reconfigure(connection,
          "timing_stress_for_test=[failpoint_rec_before_wrapup]"));
    int first_evict = ordinary_evict();
    if (strcmp(mode, "ci-random") == 0)
        CHECK(connection->reconfigure(connection, "timing_stress_for_test=[]"));
    printf("first_eviction=%d injected_failures=%u\n", first_evict, repro_failures());
    assert(first_evict == 0 || first_evict == EBUSY);

    repro_stage("boundary");
    CHECK(connection->set_timestamp(connection, "step_down_timestamp=5ceaa2"));
    CHECK(session->begin_transaction(session, NULL));
    put(base, neighbor_base, &neighbor);
    put(layered, neighbor_layered, &neighbor);
    commit(6144118);
    timestamp(6078002, 6089378);
    repro_stage("stepdown-checkpoint");
    repro_checkpoint_gate(2);
    CHECK(pthread_create(&thread, NULL, checkpoint, NULL));
    repro_checkpoint_wait();
    int second_evict = ordinary_evict();
    printf("second_eviction=%d\n", second_evict);
    assert(second_evict == 0 || second_evict == EBUSY);
    repro_checkpoint_release();
    CHECK(pthread_join(thread, NULL));

    dump_checkpoint("stepdown");
    CHECK(connection->reconfigure(connection, "disaggregated=(role=follower)"));
    pickup();
    compare("follower");
    timestamp(6361164, 6361164);
    CHECK(connection->reconfigure(connection, "disaggregated=(role=leader)"));
    CHECK(session->checkpoint(session, NULL));
    repro_stage("stepup-checkpoint-end");
    dump_checkpoint("stepup");
    compare("stepup");
    CHECK(session->begin_transaction(session, NULL));
    int b = lookup(base, victim_base), l = lookup(layered, victim_layered);
    CHECK(session->rollback_transaction(session, NULL));
    close_cursors();
    CHECK(connection->reconfigure(connection, "disaggregated=(role=follower)"));
    CHECK(connection->close(connection, NULL));
    repro_stage("persisted-open");
    snprintf(config, sizeof(config),
      "cache_size=3GB,statistics=(all),"
      "disaggregated=(role=follower,page_log=palite,stepdown_write_mirroring=%s),"
      "extensions=[\"%s/ext/page_log/palite/libwiredtiger_palite.so\"]", mirror, build);
    CHECK(wiredtiger_open(home, NULL, config, &connection));
    open_cursors();
    pickup();
    compare("persisted");
    dump_checkpoint("persisted");
    CHECK(session->begin_transaction(session, NULL));
    int persisted_base = lookup(base, victim_base);
    int persisted_layered = lookup(layered, victim_layered);
    if (persisted_base != b || persisted_layered != l) {
        fprintf(stderr, "persisted victim lookup changed\n");
        exit(1);
    }
    CHECK(session->rollback_transaction(session, NULL));
    close_cursors();
    CHECK(connection->close(connection, NULL));
    repro_stage("closed");
    repro_trace_close();
    printf("RESULT mode=%s mismatch=%u base=%d layered=%d\n", mode, b != l, b, l);
    return (b == l ? 0 : 2);
}
