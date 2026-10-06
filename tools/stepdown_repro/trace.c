/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "trace.h"

#include <pthread.h>
#include <time.h>

static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gate_cond = PTHREAD_COND_INITIALIZER;
static FILE *trace_file;
static const char *trace_mode;
static const char *trace_stage = "open";
static unsigned gate_armed, gate_entered, injected_failures;
static bool failure_armed, early_durable;
static uint64_t sequence;

/*
 * trace_flush --
 *     Fail the experiment if its diagnostic evidence cannot be written.
 */
static void
trace_flush(void)
{
    if (fflush(trace_file) != 0 || ferror(trace_file)) {
        fprintf(stderr, "repro.trace: write failed\n");
        exit(1);
    }
}

static void
item(const char *prefix, const WT_ITEM *value)
{
    if (value == NULL)
        return;
    fprintf(trace_file, " %s_size=%zu %s_hash=%" PRIu64, prefix, value->size, prefix,
      __wt_hash_city64(value->data, value->size));
    fprintf(trace_file, " %s_hex=", prefix);
    for (size_t i = 0; i < value->size; ++i)
        fprintf(trace_file, "%02x", ((const uint8_t *)value->data)[i]);
}

static void
update(const char *prefix, WT_UPDATE *upd)
{
    if (upd == NULL)
        return;
    fprintf(trace_file,
      " %s=%p %s_type=%u %s_flags=%u %s_txn=%" PRIu64 " %s_ts=%" PRIu64
      " %s_durable_ts=%" PRIu64 " %s_size=%u",
      prefix, (void *)upd, prefix, upd->type, prefix, upd->flags, prefix,
      __wt_atomic_load_uint64_v_relaxed(&upd->txnid),
      prefix, upd->upd_start_ts, prefix, upd->upd_durable_ts, prefix, upd->size);
    if (upd->type == WT_UPDATE_STANDARD)
        fprintf(trace_file, " %s_hash=%" PRIu64, prefix,
          __wt_hash_city64(upd->data, upd->size));
}

static void
block(const char *prefix, const WT_PAGE_BLOCK_META *meta)
{
    if (meta != NULL)
        fprintf(trace_file,
          " %s_page_id=%" PRIu64 " %s_lsn=%" PRIu64 " %s_base=%" PRIu64
          " %s_backlink=%" PRIu64 " %s_deltas=%u",
          prefix, meta->page_id, prefix, meta->disagg_lsn, prefix, meta->base_lsn,
          prefix, meta->backlink_lsn, prefix, meta->delta_count);
}

static void
address(const char *prefix, const WT_ADDR *addr)
{
    fprintf(trace_file, " %s_addr=%p", prefix, (const void *)addr);
    if (addr != NULL && addr->block_cookie != NULL) {
        fprintf(trace_file, " %s_cookie=", prefix);
        for (size_t i = 0; i < addr->block_cookie_size; ++i)
            fprintf(trace_file, "%02x", addr->block_cookie[i]);
    }
}

static void
page(WT_SESSION_IMPL *s, WT_REF *ref)
{
    if (ref == NULL || ref->page == NULL)
        return;
    WT_PAGE *p = ref->page;
    WT_PAGE_MODIFY *m = p->modify;
    fprintf(trace_file, " ref=%p ref_state=%u page=%p type=%u rows=%u", (void *)ref,
      WT_REF_GET_STATE(ref), (void *)p, p->type, p->entries);
    if (p->disagg_info != NULL)
        block("page", &p->disagg_info->block_meta);
    if (p->dsk != NULL)
        fprintf(trace_file, " dsk_gen=%" PRIu64 " dsk_size=%u dsk_cells=%u",
          p->dsk->write_gen, p->dsk->mem_size, p->dsk->u.entries);
    if (!__wt_ref_is_root(ref) && ref->home != NULL && s->dhandle != NULL &&
      WT_DHANDLE_BTREE(s->dhandle)) {
        WT_ADDR_COPY copy;
        WT_ENTER_GENERATION(s, WT_GEN_SPLIT);
        if (__wt_ref_addr_copy(s, ref, &copy)) {
            fprintf(trace_file, " ref_cookie=");
            for (size_t i = 0; i < copy.size; ++i)
                fprintf(trace_file, "%02x", copy.addr[i]);
        }
        WT_LEAVE_GENERATION(s, WT_GEN_SPLIT);
    }
    if (m == NULL)
        return;
    fprintf(trace_file,
      " page_state=%u rec_result=%u rec_stamp=%" PRIu64 " rec_stable=%" PRIu64
      " first_dirty_txn=%" PRIu64 " multi_entries=%u",
      m->page_state, m->rec_result, m->rec_ckpt_snap_gen, m->rec_pinned_stable_timestamp,
      m->first_dirty_txn, m->mod_multi_entries);
    if (m->rec_result == WT_PM_REC_REPLACE)
        address("replace", &m->mod_replace);
    if (m->rec_result == WT_PM_REC_MULTIBLOCK)
        for (uint32_t i = 0; i < m->mod_multi_entries; ++i) {
            block("multi", m->mod_multi[i].block_meta);
            address("multi", &m->mod_multi[i].addr);
        }
    /* Full chain inspection relies on this driver's gated, single-writer schedule. */
    if (p->type == WT_PAGE_ROW_LEAF) {
        if (m->mod_row_update != NULL)
            for (uint32_t i = 0; i < p->entries; ++i)
                for (WT_UPDATE *u = m->mod_row_update[i]; u != NULL; u = u->next)
                    update("chain", u);
        if (m->mod_row_insert != NULL)
            for (uint32_t i = 0; i <= p->entries; ++i) {
                WT_INSERT_HEAD *head = m->mod_row_insert[i];
                if (head == NULL)
                    continue;
                WT_INSERT *ins;
                WT_SKIP_FOREACH(ins, head) {
                    fprintf(trace_file, " insert_key_hash=%" PRIu64,
                      __wt_hash_city64(WT_INSERT_KEY(ins), WT_INSERT_KEY_SIZE(ins)));
                    for (WT_UPDATE *u = ins->upd; u != NULL; u = u->next)
                        update("insert", u);
                }
            }
    }
}

static void
saved(const WT_SAVE_UPD *supd)
{
    update("selected", supd->onpage_upd);
    update("tombstone", supd->onpage_tombstone);
    fprintf(trace_file, " restore=%u start=%" PRIu64 " stop=%" PRIu64,
      supd->restore, supd->tw.start_ts, supd->tw.stop_ts);
}

static int
trace(WT_SESSION_IMPL *s, const char *event, WT_REF *ref, WT_UPDATE *upd,
  const void *context, uint64_t detail)
{
    WT_CONNECTION_IMPL *conn = S2C(s);
    int action = 0;
    pthread_mutex_lock(&trace_lock);
    fprintf(trace_file,
      "seq=%" PRIu64 " stage=%s event=%s session=%u uri=%s detail=%" PRIu64
      " txn=%" PRIu64 " commit=%" PRIu64 " read=%" PRIu64 " snap_min=%" PRIu64
      " snap_max=%" PRIu64 " snap_count=%u txn_stamp=%" PRIu64 " checkpoint_gen=%" PRIu64
      " checkpoint_ts=%" PRIu64 " stable=%" PRIu64 " oldest=%" PRIu64
      " last_running=%" PRIu64 " stepdown=%" PRIu64 " stepdown_set=%u leader=%u",
      ++sequence, trace_stage, event, s->id, s->dhandle == NULL ? "none" : s->dhandle->name,
      detail, s->txn->time_point.id, s->txn->time_point.commit_timestamp,
       __wt_atomic_load_uint64_relaxed(&WT_SESSION_TXN_SHARED(s)->read_timestamp),
       s->txn->snapshot_data.snap_min,
      s->txn->snapshot_data.snap_max, s->txn->snapshot_data.snapshot_count,
       s->txn->ckpt_snap_gen, __wt_gen(s, WT_GEN_CHECKPOINT),
       __wt_atomic_load_uint64_relaxed(&conn->txn_global.checkpoint_timestamp),
       __wt_atomic_load_uint64_relaxed(&conn->txn_global.stable_timestamp),
       __wt_atomic_load_uint64_relaxed(&conn->txn_global.oldest_timestamp),
       __wt_atomic_load_uint64_v_relaxed(&conn->txn_global.last_running),
       __wt_atomic_load_uint64_relaxed(&conn->txn_global.step_down_timestamp),
       s->txn->stepdown_ts_set, __wt_atomic_load_bool_acquire(&conn->layered_table_manager.leader));
    fprintf(trace_file, " has_snapshot=%u txn_running=%u isolation=%u rng_state=%" PRIu64,
      F_ISSET(s->txn, WT_TXN_HAS_SNAPSHOT) ? 1 : 0,
      F_ISSET(s->txn, WT_TXN_RUNNING) ? 1 : 0, (unsigned)s->txn->isolation, s->rnd_random.v);
    WTI_RECONCILE *r = NULL;
    if (strncmp(event, "rec-", 4) == 0 || strncmp(event, "empty-durable", 13) == 0 ||
      strcmp(event, "reuse-address") == 0)
        r = (WTI_RECONCILE *)context;
    if (r != NULL) {
        fprintf(trace_file,
          " rec_flags=%u newer=%u leave_dirty=%u removed=%u supd_count=%u chunks=%u",
          r->flags, r->newer_updates_than_last_rec_used, r->leave_dirty,
          r->keys_removed_from_disk_image_count, r->supd_next, r->multi_next);
        for (uint32_t i = 0; i < r->supd_next; ++i) {
            fprintf(trace_file, " saved_index=%u", i);
            saved(&r->supd[i]);
        }
        for (uint32_t i = 0; i < r->multi_next; ++i) {
            WT_MULTI *multi = &r->multi[i];
            fprintf(trace_file, " product_index=%u product_flags=%u product_saved=%u", i,
              multi->flags, multi->supd_entries);
            block("product", multi->block_meta);
            address("product", &multi->addr);
            for (uint32_t j = 0; j < multi->supd_entries; ++j) {
                fprintf(trace_file, " product_saved_index=%u", j);
                saved(&multi->supd[j]);
            }
        }
        if (r->cur_ptr != NULL)
            fprintf(trace_file, " image_cells=%u image_size=%zu", r->cur_ptr->entries,
              r->cur_ptr->image.size);
    }
    if (strcmp(event, "selection") == 0 || strcmp(event, "selection-saved") == 0 ||
      strcmp(event, "selection-restore") == 0) {
        const WTI_UPDATE_SELECT *sel = context;
        update("selected", sel->upd);
        update("tombstone", sel->tombstone);
        fprintf(trace_file, " selected_saved=%u selected_start=%" PRIu64 " selected_stop=%" PRIu64,
          sel->upd_saved, sel->tw.start_ts, sel->tw.stop_ts);
    }
    if (strcmp(event, "selection-change") == 0 || strcmp(event, "restore-trim") == 0)
        saved(context);
    if (strcmp(event, "update-installed") == 0 || strncmp(event, "mirror-", 7) == 0 ||
      strcmp(event, "ingest-version") == 0)
        item("key", context);
    if (strcmp(event, "page-put") == 0)
        block("written", context);
    if (strcmp(event, "checkpoint-complete") == 0) {
        const WT_ITEM *meta = context;
        fprintf(trace_file, " completed_meta=%.*s", (int)meta->size, (const char *)meta->data);
    }
    if (strcmp(event, "page-discard") == 0 || strcmp(event, "page-discard-result") == 0) {
        const WT_BLOCK_DISAGG_ADDRESS_COOKIE *cookie = context;
        fprintf(trace_file, " discard_page_id=%" PRIu64 " discard_lsn=%" PRIu64
          " discard_base=%" PRIu64 " discard_flags=%" PRIu64,
          cookie->page_id, cookie->lsn, cookie->base_lsn, cookie->flags);
    }
    if (strcmp(event, "restore-image") == 0 || strcmp(event, "durable-before") == 0 ||
      strcmp(event, "durable-after") == 0) {
        const WT_MULTI *multi = context;
        block("product", multi->block_meta);
        address("product", &multi->addr);
    }
    if (strcmp(event, "layered-route") == 0) {
        const WTI_CLAYERED_OP *op = context;
        fprintf(trace_file, " target=%u stable_cursor=%p ingest_cursor=%p", op->write_target,
          (void *)op->stable, (void *)op->ingest);
    }
    update("update", upd);
    page(s, ref);
    bool stable_tree = s->dhandle != NULL && strstr(s->dhandle->name, "T00002.wt_stable") != NULL;
    if (stable_tree && strcmp(event, "empty-durable-after") == 0)
        early_durable = true;
    if (stable_tree && strcmp(event, "rec-before-wrapup") == 0 && failure_armed && early_durable) {
        failure_armed = false;
        early_durable = false;
        ++injected_failures;
        action = EBUSY;
        fprintf(trace_file, " intervention=fail-before-wrapup");
    }
    if (stable_tree && strcmp(event, "checkpoint-skip") == 0 && strcmp(trace_mode, "no-skip") == 0) {
        action = 1;
        fprintf(trace_file, " intervention=decline-checkpoint-skip");
    }
    fputc('\n', trace_file);
    trace_flush();
    if (stable_tree && strcmp(event, "checkpoint-tree-begin") == 0 && gate_armed != 0) {
        gate_entered = gate_armed;
        pthread_cond_broadcast(&gate_cond);
        while (gate_armed != 0)
            pthread_cond_wait(&gate_cond, &trace_lock);
    }
    pthread_mutex_unlock(&trace_lock);
    return (action);
}

void
repro_trace_open(const char *home, const char *mode)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/repro.trace", home);
    trace_file = fopen(path, "w");
    if (trace_file == NULL) {
        perror(path);
        exit(1);
    }
    trace_mode = mode;
    __wt_process.stepdown_repro_trace = trace;
}

void
repro_trace_close(void)
{
    __wt_process.stepdown_repro_trace = NULL;
    trace_flush();
    if (fclose(trace_file) != 0) {
        fprintf(stderr, "repro.trace: close failed\n");
        exit(1);
    }
}

void
repro_checkpoint_gate(unsigned gate)
{
    pthread_mutex_lock(&trace_lock);
    gate_armed = gate;
    gate_entered = 0;
    pthread_mutex_unlock(&trace_lock);
}

void
repro_checkpoint_wait(void)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 30;
    pthread_mutex_lock(&trace_lock);
    while (gate_entered == 0) {
        int ret = pthread_cond_timedwait(&gate_cond, &trace_lock, &deadline);
        if (ret != 0) {
            fprintf(stderr, "checkpoint gate wait: %s\n", strerror(ret));
            exit(1);
        }
    }
    pthread_mutex_unlock(&trace_lock);
}

void
repro_checkpoint_release(void)
{
    pthread_mutex_lock(&trace_lock);
    gate_armed = 0;
    pthread_cond_broadcast(&gate_cond);
    pthread_mutex_unlock(&trace_lock);
}

void
repro_failure_arm(void)
{
    pthread_mutex_lock(&trace_lock);
    failure_armed = strcmp(trace_mode, "no-fail") != 0 && strcmp(trace_mode, "ci-random") != 0;
    early_durable = false;
    pthread_mutex_unlock(&trace_lock);
}

void
repro_stage(const char *stage)
{
    pthread_mutex_lock(&trace_lock);
    trace_stage = stage;
    fprintf(trace_file, "seq=%" PRIu64 " event=stage stage=%s\n", ++sequence, stage);
    trace_flush();
    pthread_mutex_unlock(&trace_lock);
}

void
repro_note(WT_SESSION_IMPL *session, const char *event, WT_REF *ref, uint64_t detail)
{
    (void)trace(session, event, ref, NULL, NULL, detail);
}

unsigned
repro_failures(void)
{
    return (injected_failures);
}
