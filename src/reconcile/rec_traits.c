#include "wt_internal.h"
#include "rec_traits_details.h"

const struct __wt_rec_traits REC_PAGE_COL_FIX_TRAITS = {
  &CACHE_LAS_COL_FIX_TRAITS, __rec_page_col_fix};
const struct __wt_rec_traits REC_PAGE_COL_INT_TRAITS = {NULL, __rec_page_col_int};
const struct __wt_rec_traits REC_PAGE_COL_VAR_TRAITS = {
  &CACHE_LAS_COL_VAR_TRAITS, __wt_rec_col_var};
const struct __wt_rec_traits REC_PAGE_ROW_INT_TRAITS = {NULL, __rec_page_row_int};
const struct __wt_rec_traits REC_PAGE_ROW_LEAF_TRAITS = {&CACHE_LAS_ROW_TRAITS, __wt_rec_row_leaf};
