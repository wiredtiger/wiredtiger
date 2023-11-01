/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __wt_config {
    WT_SESSION_IMPL *session;
    const char *orig;
    const char *end;
    const char *cur;

    int depth, top;
    const int8_t *go;
};

/*
 * We have jump tables that for each ASCII character, show the offset in a lookup table that keys
 * starting with that character start at. All keys are 7 bit ASCII.
 */
#define WT_CONFIG_JUMP_TABLE_SIZE 128

/*
 * The expected types of a configuration value.
 */
#define WT_CONFIG_COMPILED_TYPE_INT 0
#define WT_CONFIG_COMPILED_TYPE_BOOLEAN 1
#define WT_CONFIG_COMPILED_TYPE_FORMAT 2
#define WT_CONFIG_COMPILED_TYPE_STRING 3
#define WT_CONFIG_COMPILED_TYPE_CATEGORY 4
#define WT_CONFIG_COMPILED_TYPE_LIST 5

struct __wt_config_check {
    const char *name;
    const char *type;
    int (*checkf)(WT_SESSION_IMPL *, WT_CONFIG_ITEM *);
    const char *checks;
    const WT_CONFIG_CHECK *subconfigs;
    u_int subconfigs_entries;
    const uint8_t *subconfigs_jump;
    u_int compiled_type;
    int64_t min_value;
    int64_t max_value;
    const char **choices;
};

#define WT_CONFIG_REF(session, n) (S2C(session)->config_entries[WT_CONFIG_ENTRY_##n])
struct __wt_config_entry {
    const char *method; /* method name */

#define WT_CONFIG_BASE(session, n) (WT_CONFIG_REF(session, n)->base)
    const char *base; /* configuration base */

    const WT_CONFIG_CHECK *checks; /* check array */
    u_int checks_entries;
    const uint8_t *checks_jump;
};

struct __wt_config_parser_impl {
    WT_CONFIG_PARSER iface;

    WT_SESSION_IMPL *session;
    WT_CONFIG config;
    WT_CONFIG_ITEM config_item;
};

#define WT_CONFIG_ITEM_STATIC_INIT(n) static const WT_CONFIG_ITEM n = {"", 0, 0, WT_CONFIG_ITEM_NUM}

/*
 * If double quotes surround the string, then expand the string to include them. This is always
 * called in the context of keys or values returned by the configuration parser. The character after
 * the string must be at a valid memory address, and checking just that one is sufficient. If it is
 * a double quote, then the character before must be as well, by the rules of the tokenizer.
 */
#define WT_CONFIG_PRESERVE_QUOTES(session, item)        \
    do {                                                \
        if ((item)->str[(item)->len] == '"') {          \
            WT_ASSERT(session, (item)->str[-1] == '"'); \
            (item)->str -= 1;                           \
            (item)->len += 2;                           \
        }                                               \
    } while (0)

#define WT_CONFIG_UNSET (-1)
/*
 * DO NOT EDIT: automatically built by dist/api_config.py.
 * configuration section: BEGIN
 */
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_collator 0
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_compressor 1
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_data_source 2
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_encryptor 3
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_extractor 4
#define WT_CONFIG_ENTRY_WT_CONNECTION_add_storage_source 5
#define WT_CONFIG_ENTRY_WT_CONNECTION_close 6
#define WT_CONFIG_ENTRY_WT_CONNECTION_debug_info 7
#define WT_CONFIG_ENTRY_WT_CONNECTION_load_extension 8
#define WT_CONFIG_ENTRY_WT_CONNECTION_open_session 9
#define WT_CONFIG_ENTRY_WT_CONNECTION_query_timestamp 10
#define WT_CONFIG_ENTRY_WT_CONNECTION_reconfigure 11
#define WT_CONFIG_ENTRY_WT_CONNECTION_rollback_to_stable 12
#define WT_CONFIG_ENTRY_WT_CONNECTION_set_file_system 13
#define WT_CONFIG_ENTRY_WT_CONNECTION_set_timestamp 14
#define WT_CONFIG_ENTRY_WT_CURSOR_bound 15
#define WT_CONFIG_ENTRY_WT_CURSOR_close 16
#define WT_CONFIG_ENTRY_WT_CURSOR_reconfigure 17
#define WT_CONFIG_ENTRY_WT_SESSION_alter 18
#define WT_CONFIG_ENTRY_WT_SESSION_begin_transaction 19
#define WT_CONFIG_ENTRY_WT_SESSION_checkpoint 20
#define WT_CONFIG_ENTRY_WT_SESSION_close 21
#define WT_CONFIG_ENTRY_WT_SESSION_commit_transaction 22
#define WT_CONFIG_ENTRY_WT_SESSION_compact 23
#define WT_CONFIG_ENTRY_WT_SESSION_create 24
#define WT_CONFIG_ENTRY_WT_SESSION_drop 25
#define WT_CONFIG_ENTRY_WT_SESSION_join 26
#define WT_CONFIG_ENTRY_WT_SESSION_log_flush 27
#define WT_CONFIG_ENTRY_WT_SESSION_log_printf 28
#define WT_CONFIG_ENTRY_WT_SESSION_open_cursor 29
#define WT_CONFIG_ENTRY_WT_SESSION_prepare_transaction 30
#define WT_CONFIG_ENTRY_WT_SESSION_query_timestamp 31
#define WT_CONFIG_ENTRY_WT_SESSION_reconfigure 32
#define WT_CONFIG_ENTRY_WT_SESSION_rename 33
#define WT_CONFIG_ENTRY_WT_SESSION_reset 34
#define WT_CONFIG_ENTRY_WT_SESSION_reset_snapshot 35
#define WT_CONFIG_ENTRY_WT_SESSION_rollback_transaction 36
#define WT_CONFIG_ENTRY_WT_SESSION_salvage 37
#define WT_CONFIG_ENTRY_WT_SESSION_strerror 38
#define WT_CONFIG_ENTRY_WT_SESSION_timestamp_transaction 39
#define WT_CONFIG_ENTRY_WT_SESSION_timestamp_transaction_uint 40
#define WT_CONFIG_ENTRY_WT_SESSION_truncate 41
#define WT_CONFIG_ENTRY_WT_SESSION_upgrade 42
#define WT_CONFIG_ENTRY_WT_SESSION_verify 43
#define WT_CONFIG_ENTRY_colgroup_meta 44
#define WT_CONFIG_ENTRY_file_config 45
#define WT_CONFIG_ENTRY_file_meta 46
#define WT_CONFIG_ENTRY_index_meta 47
#define WT_CONFIG_ENTRY_lsm_meta 48
#define WT_CONFIG_ENTRY_object_meta 49
#define WT_CONFIG_ENTRY_table_meta 50
#define WT_CONFIG_ENTRY_tier_meta 51
#define WT_CONFIG_ENTRY_tiered_meta 52
#define WT_CONFIG_ENTRY_wiredtiger_open 53
#define WT_CONFIG_ENTRY_wiredtiger_open_all 54
#define WT_CONFIG_ENTRY_wiredtiger_open_basecfg 55
#define WT_CONFIG_ENTRY_wiredtiger_open_usercfg 56
/*
 * configuration section: END
 * DO NOT EDIT: automatically built by dist/flags.py.
 */
