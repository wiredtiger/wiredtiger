/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
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
#define FOLLOWER_RECORDS_FILE RECORDS_DIR DIR_DELIM_STR "follower-%" PRIu32
#define SCHEMA_DATA_FILE RECORDS_DIR DIR_DELIM_STR "data-%" PRIu32
#define FOLLOWER_HOME_DIR "WT_FOLLOWER"
#define DATA_KEY "k"

/* Connection config. */
#define ENV_CONFIG_DEF                                                                             \
    "create,"                                                                                      \
    "eviction_updates_trigger=95,eviction_updates_target=80,"                                      \
    "log=(enabled,file_max=10M,remove=false),statistics=(all),statistics_log=(json,on_close,wait=" \
    "1)"

#define ENV_CONFIG_SWEEP \
    ",file_manager=(close_handle_minimum=0,close_idle_time=1,close_scan_interval=1)"

/* Schema event types. */
typedef enum {
    SCHEMA_OP_CREATE,
    SCHEMA_OP_DROP,
    SCHEMA_OP_CKPT,
    SCHEMA_OP_EOF
} SCHEMA_OP_TYPE;

#define SCHEMA_URI_MAX 256

typedef struct {
    SCHEMA_OP_TYPE type;
    uint32_t thread_id;
    uint64_t epoch;
    char uri[SCHEMA_URI_MAX];
} SCHEMA_EVENT;

/* Multi-producer, single-consumer queue. */
#define SCHEMA_QUEUE_SIZE 1024

typedef struct {
    SCHEMA_EVENT buf[SCHEMA_QUEUE_SIZE];
    uint64_t head;
    uint64_t tail;
    pthread_mutex_t lock;
} SCHEMA_QUEUE;

/* Kill target. */
typedef enum { KILL_LEADER, KILL_FOLLOWER, KILL_BOTH } KILL_MODE;

/* Per-thread argument. */
typedef struct {
    WT_CONNECTION *conn;
    uint32_t info;
    WT_RAND_STATE rnd;
} THREAD_DATA;

/* Globals shared across translation units. */
extern char home[1024];
extern char page_log_home[PATH_MAX];
extern int schema_pipe[2];

extern bool aggressive_sweep;
extern volatile bool stable_set;
extern uint32_t nth;
extern uint64_t schema_op_epoch;

extern SCHEMA_QUEUE schema_queue;
extern pthread_mutex_t schema_publish_lock;

extern KILL_MODE kill_mode;
extern TEST_OPTS *opts;

/* Phase 2 (role-switch): enabled by -w; run_timeout drives the switch timer. */
extern bool role_switch;
extern uint32_t run_timeout;
/* Phase 2 (role-switch): set by the switch timer to quiesce all worker threads. */
extern volatile bool stop_workload;

extern const char *const ready_file;
extern const char *const follower_ready_file;
extern const char *const follower_stepped_up_file;

/* workload.c */
void schema_queue_push(const SCHEMA_EVENT *ev);
bool schema_queue_pop(SCHEMA_EVENT *ev);
void run_workload(void) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));
void run_follower(int pipe_rd) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/* verify.c */
bool verify_schema_state(WT_CONNECTION *conn, const char *records_fmt);
