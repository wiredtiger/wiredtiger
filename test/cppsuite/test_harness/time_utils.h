#ifndef TIME_UTILS_H
#define TIME_UTILS_H

#include <ctime>

/* Define helpful functions related to time. */
namespace test_harness {

#define SEC_TO_MS (1000)

static time_t
get_time()
{
    time_t now = time(nullptr);
    return (now * SEC_TO_MS);
}

} // namespace test_harness

#endif
