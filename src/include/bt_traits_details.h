extern int __bt_col_fix_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_col_var_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_row_huffman(WT_SESSION_IMPL *session, size_t len);
extern int __bt_col_fix_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
extern int __bt_col_var_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
extern int __bt_row_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid);
#ifdef HAVE_DIAGNOSTIC
extern int __bt_col_cursor_key_order_init(WT_CURSOR_BTREE *cbt);
extern int __bt_row_cursor_key_order_init(WT_CURSOR_BTREE *cbt);
extern int __bt_col_cursor_key_order_check(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next);
extern int __bt_row_cursor_key_order_check(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next);
extern void __bt_cursor_key_order_reset(WT_CURSOR_BTREE *cbt);
#endif
