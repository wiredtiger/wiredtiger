#include <cstdio>
#include <string>
#include <stdexcept>

#include "utils.h"

namespace utils {

void
throwIfNonZero(int result)
{
    if (result != 0) {
        std::string errorMessage("Error result is " + std::to_string(result));
        throw std::runtime_error(errorMessage);
    }
}

void
wiredtigerCleanup()
{
    // ignoring errors here; we don't mind if something doesn't exist
    std::remove("WiredTiger");
    std::remove("WiredTiger.basecfg");
    std::remove("WiredTiger.lock");
    std::remove("WiredTiger.turtle");
    std::remove("WiredTiger.wt");
    std::remove("WiredTigerHS.wt");
}

} // namespace utils
