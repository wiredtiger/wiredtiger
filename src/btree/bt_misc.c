
/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_cell_type_string --
 *     Return a string representing the cell type.
 */
const char *
__wti_cell_type_string(uint8_t type)
{
    switch (type) {
    case WT_CELL_ADDR_DEL:
        return ("addr_del");
    case WT_CELL_ADDR_INT:
        return ("addr_int");
    case WT_CELL_ADDR_LEAF:
        return ("addr_leaf");
    case WT_CELL_ADDR_LEAF_NO:
        return ("addr_leaf_no_ovfl");
    case WT_CELL_DEL:
        return ("deleted");
    case WT_CELL_KEY:
        return ("key");
    case WT_CELL_KEY_PFX:
        return ("key_pfx");
    case WT_CELL_KEY_OVFL:
        return ("key_ovfl");
    case WT_CELL_KEY_SHORT:
        return ("key_short");
    case WT_CELL_KEY_SHORT_PFX:
        return ("key_short_pfx");
    case WT_CELL_KEY_OVFL_RM:
        return ("key_ovfl_rm");
    case WT_CELL_VALUE:
        return ("value");
    case WT_CELL_VALUE_COPY:
        return ("value_copy");
    case WT_CELL_VALUE_OVFL:
        return ("value_ovfl");
    case WT_CELL_VALUE_OVFL_RM:
        return ("value_ovfl_rm");
    case WT_CELL_VALUE_SHORT:
        return ("value_short");
    default:
        return ("unknown");
    }
    /* NOTREACHED */
}

/*
 * __wt_key_string --
 *     Load a buffer with a printable, nul-terminated representation of a key.
 */
const char *
__wt_key_string(
  WT_SESSION_IMPL *session, const void *data_arg, size_t size, const char *key_format, WT_ITEM *buf)
{
    WT_ITEM tmp;

#ifdef HAVE_DIAGNOSTIC
    if (session->dump_raw)
        return (__wt_buf_set_printable(session, data_arg, size, false, buf));
#endif

    /*
     * If the format is 'S', it's a string and our version of it may not yet be nul-terminated.
     */
    if (WT_STREQ(key_format, "S") && ((char *)data_arg)[size - 1] != '\0') {
        WT_CLEAR(tmp);
        if (__wt_buf_fmt(session, &tmp, "%.*s", (int)size, (char *)data_arg) == 0) {
            data_arg = tmp.data;
            size = tmp.size + 1;
        } else {
            data_arg = WT_ERR_STRING;
            size = sizeof(WT_ERR_STRING);
        }
    }
    return (__wt_buf_set_printable_format(session, data_arg, size, key_format, false, buf));
}

