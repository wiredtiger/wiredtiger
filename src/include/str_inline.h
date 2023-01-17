/*
 * __wt_prepare_state_str --
 *     Convert a prepare state to its string representation.
 */
static inline const char *
__wt_prepare_state_str(uint8_t state)
{
    switch (state) {
    case WT_PREPARE_INIT:
        return ("PREPARE_INIT");
    case WT_PREPARE_INPROGRESS:
        return ("PREPARE_INPROGRESS");
    case WT_PREPARE_LOCKED:
        return ("PREPARE_LOCKED");
    case WT_PREPARE_RESOLVED:
        return ("PREPARE_RESOLVED");
    }

    return ("PREPARE_INVALID");
}

/*
 * __wt_update_type_str --
 *     Convert an update type to its string representation.
 */
static inline const char *
__wt_update_type_str(uint8_t ty)
{
    switch (ty) {
    case WT_UPDATE_MODIFY:
        return ("UPDATE_MODIFY");
    case WT_UPDATE_RESERVE:
        return ("UPDATE_RESERVE");
    case WT_UPDATE_STANDARD:
        return ("UPDATE_STANDARD");
    case WT_UPDATE_TOMBSTONE:
        return ("UPDATE_TOMBSTONE");
    }

    return ("UPDATE_INVALID");
}
