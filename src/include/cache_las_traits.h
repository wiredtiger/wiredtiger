struct __wt_cache_las_traits {
    int (*get_key)(
      WT_SESSION_IMPL *session, WT_BTREE *btree, WT_PAGE *page, WT_SAVE_UPD *list, WT_ITEM *key);
};

extern const struct __wt_cache_las_traits CACHE_LAS_COL_FIX_TRAITS;
extern const struct __wt_cache_las_traits CACHE_LAS_COL_VAR_TRAITS;
extern const struct __wt_cache_las_traits CACHE_LAS_ROW_TRAITS;
