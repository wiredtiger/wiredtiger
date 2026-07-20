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

/*
 * Disaggregated schema epoch crash recovery test.
 *
 * The test binary runs in one of three roles, each in its own process:
 *
 *   parent   - orchestrator and verifier. Spawns the children, kills them per the configured mode,
 *              then reopens the surviving state and verifies it against the record files.
 *   leader   - runs schema worker threads that create, drop, publish, and populate layered tables
 *              while checkpointing, until killed.
 *   follower - tracks the leader through schema events and checkpoint pickups; steps up when the
 *              leader dies in kill-leader mode.
 *
 * The children are started by re-spawning this binary with an internal role option, so no state is
 * inherited by forking: everything a child needs travels through the command line. The leader
 * relays events to the follower over a pipe whose descriptor numbers are likewise passed on the
 * command line.
 */

#pragma once

#include "test_util.h"

/* Tunables. */
#define MAX_CKPT_INVL 4
#define MAX_POOL_SIZE 64
#define MAX_STARTUP 60
#define MAX_TH 12
#define MAX_TIME 40
#define MIN_POOL_SIZE 2
#define MIN_TH 2
#define MIN_TIME 10

/* URI / file name patterns. */
#define DATA_KEY_MIN 0
#define DATA_KEY_MAX 9
#define LEADER_READY_FILE "leader_ready"
#define SWITCH_DONE_FILE "switch_done" /* switch mode: the role transition has completed */
#define SCHEMA_TABLE_FMT "table:schema_%u_%u"

/*
 * Per-thread record files: "<records dir>/<base>-<thread>". The single-format/base-name split keeps
 * every snprintf format a literal so the compiler can check it.
 */
#define RECORDS_FILE_FMT RECORDS_DIR DIR_DELIM_STR "%s-%" PRIu32
#define SCHEMA_RECORDS_BASE "schema"
#define SCHEMA_RECORDS_FILE RECORDS_DIR DIR_DELIM_STR SCHEMA_RECORDS_BASE "-%" PRIu32

/* Multi-node mode: follower home, sentinels and record files. */
#define FOLLOWER_HOME_DIR "WT_FOLLOWER"
#define FOLLOWER_READY_FILE "follower_ready"
#define FOLLOWER_STEPPED_UP_FILE "follower_stepped_up"
#define FOLLOWER_RECORDS_BASE "follower"
#define FOLLOWER_RECORDS_FILE RECORDS_DIR DIR_DELIM_STR FOLLOWER_RECORDS_BASE "-%" PRIu32

/* Connection config. */
#define ENV_CONFIG_DEF "create,statistics=(all),statistics_log=(json,on_close,wait=1)"

/* Which process this instance of the binary is. */
typedef enum { ROLE_PARENT = 0, ROLE_LEADER, ROLE_FOLLOWER } TEST_ROLE;

/* Which child processes the parent kills in multi-node mode. */
typedef enum { KILL_NONE = 0, KILL_LEADER, KILL_FOLLOWER, KILL_BOTH } KILL_MODE;

/* Schema event relayed from the leader to the follower over the pipe. */
typedef enum { EVENT_CREATE, EVENT_DROP, EVENT_INSERT, EVENT_CKPT } EVENT_TYPE;

typedef struct {
    EVENT_TYPE type;
    uint32_t thread_id;
    uint64_t epoch;
    uint64_t commit_ts;
    uint32_t key_min;
    uint32_t key_max;
    char uri[64];
} SCHEMA_EVENT;

/* Test-wide configuration, built from the command line by every role independently. */
typedef struct {
    TEST_OPTS *opts;
    TEST_ROLE role;
    char home[PATH_MAX];
    char page_log_home[PATH_MAX];
    uint32_t nth;
    uint32_t pool_size;
    uint32_t timeout;
    KILL_MODE kill_mode; /* KILL_NONE runs the single-node scenario */
    bool switch_mode;    /* single-node: random starting role, then a role switch mid-run */
    bool verify_only;
    int pipe_read_fd;  /* -1 when single-node */
    int pipe_write_fd; /* -1 when single-node */
} TEST_CONFIG;

/* parent.c */
void parent_main(TEST_CONFIG *cfg, const char *self_path);

/* leader.c */
void leader_main(TEST_CONFIG *cfg) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/* follower.c */
void follower_main(TEST_CONFIG *cfg) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn));

/* verify.c */
void verify_schema_state(WT_CONNECTION *conn, const TEST_CONFIG *cfg, const char *records_base);
