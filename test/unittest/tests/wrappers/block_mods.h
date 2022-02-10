#ifndef WT_BLOCKMODS_H
#define WT_BLOCKMODS_H

#include "wt_internal.h"

class BlockMods {
    public:
    BlockMods();
    ~BlockMods();
    WT_BLOCK_MODS *
    getWTBlockMods()
    {
        return &_block_mods;
    };

    private:
    void initBlockMods();
    WT_BLOCK_MODS _block_mods;
};

#endif // WT_BLOCKMODS_H
