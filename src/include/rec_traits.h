struct __wt_rec_traits {
    const WT_CACHE_LAS_TRAITS *cache_las_traits;
    int (*page_reconcile)(
      WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *ref, WT_SALVAGE_COOKIE *salvage);
};

extern const struct __wt_rec_traits REC_PAGE_COL_FIX_TRAITS;
extern const struct __wt_rec_traits REC_PAGE_COL_INT_TRAITS;
extern const struct __wt_rec_traits REC_PAGE_COL_VAR_TRAITS;
extern const struct __wt_rec_traits REC_PAGE_ROW_INT_TRAITS;
extern const struct __wt_rec_traits REC_PAGE_ROW_LEAF_TRAITS;
