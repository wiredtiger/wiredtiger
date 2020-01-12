#include "wt_internal.h"
#include "bt_traits_details.h"

const struct __wt_bt_traits BT_COL_FIX_TRAITS = {__bt_col_fix_huffman, __bt_col_fix_cursor_valid};
const struct __wt_bt_traits BT_COL_VAR_TRAITS = {__bt_col_var_huffman, __bt_col_var_cursor_valid};
const struct __wt_bt_traits BT_ROW_TRAITS = {__bt_row_huffman, __bt_row_cursor_valid};
