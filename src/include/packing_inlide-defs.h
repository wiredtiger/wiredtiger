// This is a multi-inclusion file, do not directly include this file.

PACK_TYPEDEF_BEGIN(pack_row_truncate)
    PACK_FIELD(I, uint32_t, optype)
    PACK_FIELD(I, uint32_t, recsize)
    PACK_FIELD(I, uint32_t, fileid)
    PACK_FIELD(u, WT_ITEM, start)
    PACK_FIELD(u, WT_ITEM, stop)
    PACK_FIELD(I, uint32_t, mode)
PACK_TYPEDEF_END()
