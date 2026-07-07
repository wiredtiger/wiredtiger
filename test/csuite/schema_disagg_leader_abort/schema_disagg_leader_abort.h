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

#pragma once

#include "test_util.h"

#include <pthread.h>
#include <sys/wait.h>
#include <signal.h>

/* Tunables. */
#define MAX_CKPT_INVL 4
#define MAX_STARTUP 60
#define MAX_TH 12
#define MAX_TIME 40
#define MIN_TH 2
#define MIN_TIME 10

/* URI / file name patterns. */
#define SCHEMA_POOL_SIZE 8
#define SCHEMA_TABLE_FMT "table:schema_%u_%u"
#define SCHEMA_RECORDS_FILE RECORDS_DIR DIR_DELIM_STR "schema-%" PRIu32
#define SCHEMA_DATA_FILE RECORDS_DIR DIR_DELIM_STR "data-%" PRIu32
#define DATA_KEY "k"

/* Connection config. */
#define ENV_CONFIG_DEF                                                                             \
    "create,"                                                                                      \
    "eviction_updates_trigger=95,eviction_updates_target=80,"                                      \
    "log=(enabled,file_max=10M,remove=false),statistics=(all),statistics_log=(json,on_close,wait=" \
    "1)"

#define ENV_CONFIG_SWEEP \
    ",file_manager=(close_handle_minimum=0,close_idle_time=1,close_scan_interval=1)"

/* Per-thread argument. */
typedef struct {
    WT_CONNECTION *conn;
    uint32_t info;
    WT_RAND_STATE rnd;
} THREAD_DATA;

/* Globals shared across translation units. */
extern char home[1024];
extern char page_log_home[PATH_MAX];

extern bool aggressive_sweep;
extern volatile bool stable_set;
extern uint32_t nth;
extern uint64_t schema_op_epoch;

extern pthread_mutex_t schema_publish_lock;

extern TEST_OPTS *opts;

extern const char *const ready_file;

/* workload.c */
void run_workload(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/* verify.c */
bool verify_schema_state(WT_CONNECTION *conn);
