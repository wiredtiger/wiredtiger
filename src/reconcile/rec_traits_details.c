#include "wt_internal.h"
#include "rec_traits_details.h"

/*
 * __rec_page_col_fix --
 *     Reconcile page for col fix.
 */
int
__rec_page_col_fix(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *ref, WT_SALVAGE_COOKIE *salvage)
{
    if (salvage != NULL)
        WT_RET(__wt_rec_col_fix_slvg(session, r, ref, salvage));
    else
        WT_RET(__wt_rec_col_fix(session, r, ref));
    return (0);
}

/*
 * __rec_page_col_int --
 *     Reconcile page for col int.
 */
int
__rec_page_col_int(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *ref, WT_SALVAGE_COOKIE *salvage)
{
    WT_DECL_RET;

    (void)salvage; /* Unused */
    WT_WITH_PAGE_INDEX(session, ret = __wt_rec_col_int(session, r, ref));
    return ret;
}

/*
 * __rec_page_row_int --
 *     Reconcile page for row int.
 */
int
__rec_page_row_int(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REF *ref, WT_SALVAGE_COOKIE *salvage)
{
    WT_DECL_RET;

    (void)salvage; /* Unused */
    WT_WITH_PAGE_INDEX(session, ret = __wt_rec_row_int(session, r, ref->page));
    return ret;
}
