struct __wt_bt_traits {
    int (*huffman)(WT_SESSION_IMPL *session, size_t len);
    int (*cursor_valid)(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
};

extern const struct __wt_bt_traits BT_COL_FIX_TRAITS;
extern const struct __wt_bt_traits BT_COL_VAR_TRAITS;
extern const struct __wt_bt_traits BT_ROW_TRAITS;