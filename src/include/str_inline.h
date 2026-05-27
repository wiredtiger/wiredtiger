#pragma once

/*
 * __wt_prepare_state_str --
 *     Convert a prepare state to its string representation.
 */
static WT_INLINE const char *
__wt_prepare_state_str(uint8_t val)
{
    switch (val) {
    case WT_PREPARE_INIT:
        return ("WT_PREPARE_INIT");
    case WT_PREPARE_INPROGRESS:
        return ("WT_PREPARE_INPROGRESS");
    case WT_PREPARE_LOCKED:
        return ("WT_PREPARE_LOCKED");
    case WT_PREPARE_RESOLVED:
        return ("WT_PREPARE_RESOLVED");
    }

    return ("PREPARE_STATE_INVALID");
}

/*
 * __wt_update_type_str --
 *     Convert an update type to its string representation.
 */
static WT_INLINE const char *
__wt_update_type_str(uint8_t val)
{
    switch (val) {
    case WT_UPDATE_INVALID:
        return ("WT_UPDATE_INVALID");
    case WT_UPDATE_MODIFY:
        return ("WT_UPDATE_MODIFY");
    case WT_UPDATE_RESERVE:
        return ("WT_UPDATE_RESERVE");
    case WT_UPDATE_STANDARD:
        return ("WT_UPDATE_STANDARD");
    case WT_UPDATE_TOMBSTONE:
        return ("WT_UPDATE_TOMBSTONE");
    }

    return ("UPDATE_TYPE_INVALID");
}

/*
 * __wt_page_type_str --
 *     Convert a page type to its string representation.
 */
static WT_INLINE const char *
__wt_page_type_str(uint8_t val)
{
    switch (val) {
    case WT_PAGE_INVALID:
        return ("WT_PAGE_INVALID");
    case WT_PAGE_BLOCK_MANAGER:
        return ("WT_PAGE_BLOCK_MANAGER");
    case WT_PAGE_COL_FIX_DEPRECATED:
        return ("WT_PAGE_COL_FIX_DEPRECATED");
    case WT_PAGE_COL_INT:
        return ("WT_PAGE_COL_INT");
    case WT_PAGE_COL_VAR:
        return ("WT_PAGE_COL_VAR");
    case WT_PAGE_OVFL:
        return ("WT_PAGE_OVFL");
    case WT_PAGE_ROW_INT:
        return ("WT_PAGE_ROW_INT");
    case WT_PAGE_ROW_LEAF:
        return ("WT_PAGE_ROW_LEAF");
    case WT_PAGE_TYPE_COUNT:
        return ("WT_PAGE_TYPE_COUNT");
    }

    return ("PAGE_TYPE_INVALID");
}

/*
 * __wt_page_type_valid --
 *     Return true if the given byte is a known, in-range page type. WT_PAGE_INVALID (0) and any
 *     value at or above WT_PAGE_TYPE_COUNT are rejected. Intended as a cheap guard in front of
 *     every switch (page->type) / switch (dsk->type) site, complementing the write-side check added
 *     in WT-14750 (see __rec_write).
 */
static WT_INLINE bool
__wt_page_type_valid(uint8_t type)
{
    return (type != WT_PAGE_INVALID && type < WT_PAGE_TYPE_COUNT);
}
