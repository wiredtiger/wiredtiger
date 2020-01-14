struct __wt_bt_traits {
    int (*huffman)(WT_SESSION_IMPL *session, size_t len);
    int (*cursor_valid)(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
#ifdef HAVE_DIAGNOSTIC
    int (*cursor_key_order_init)(WT_CURSOR_BTREE *cbt);
    int (*cursor_key_order_check)(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next);
    void (*cursor_key_order_reset)(WT_CURSOR_BTREE *cbt);
#endif
};

extern const struct __wt_bt_traits BT_COL_FIX_TRAITS;
extern const struct __wt_bt_traits BT_COL_VAR_TRAITS;
extern const struct __wt_bt_traits BT_ROW_TRAITS;
