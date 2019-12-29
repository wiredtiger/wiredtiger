#include "wt_internal.h"

static int __las_key_col(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_PAGE *page, WT_SAVE_UPD *list, WT_ITEM *key);
static int __las_key_row(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_PAGE *page, WT_SAVE_UPD *list, WT_ITEM *key);

/*
 * __las_key_col --
 *     Get las key for col store.
 */
static int
__las_key_col(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_PAGE *page, WT_SAVE_UPD *list, WT_ITEM *key)
{
    uint8_t *p;
    /* Unused parameters */
    (void)session;
    (void)btree;
    (void)page;

    p = key->mem;
    WT_RET(__wt_vpack_uint(&p, 0, WT_INSERT_RECNO(list->ins)));
    key->size = WT_PTRDIFF(p, key->data);
    return (0);
}

/*
 * __las_key_row --
 *     Get las key for row store.
 */
static int
__las_key_row(
  WT_SESSION_IMPL *session, WT_BTREE *btree, WT_PAGE *page, WT_SAVE_UPD *list, WT_ITEM *key)
{
    WT_DECL_RET;
    if (list->ins == NULL) {
        WT_WITH_BTREE(
          session, btree, ret = __wt_row_leaf_key(session, page, list->ripcip, key, false));
        WT_RET(ret);
    } else {
        key->data = WT_INSERT_KEY(list->ins);
        key->size = WT_INSERT_KEY_SIZE(list->ins);
    }
    return (0);
}

const struct __wt_cache_las_traits CACHE_LAS_COL_FIX_TRAITS = {__las_key_col};
const struct __wt_cache_las_traits CACHE_LAS_COL_VAR_TRAITS = {__las_key_col};
const struct __wt_cache_las_traits CACHE_LAS_ROW_TRAITS = {__las_key_row};
