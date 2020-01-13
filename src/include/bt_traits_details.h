extern int __bt_col_fix_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_col_var_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_row_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_col_fix_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
extern int __bt_col_var_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
extern int __bt_row_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
