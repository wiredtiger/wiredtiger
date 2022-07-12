/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "timestamp_simulator.h"
#include <iostream>

int
timestamp_simulator::set_ts(int ts)
{
    timestamp = ts;
    std::cout << "timestamp_simulator::set_ts: " << timestamp << std::endl;
    return 0;
}

int
timestamp_simulator::get_ts()
{
    return timestamp;
}

oldest_timestamp::oldest_timestamp()
{
    std::cout << "Creating oldest timestamp" << std::endl;
}

void
oldest_timestamp::get_specs()
{
    std::cout << "=== Oldest Timestamp ===" << std::endl;
    std::cout << "Constraints:" << std::endl;
    std::cout << "<= stable; may not move backward, set to the value as of the last checkpoint "
                 "during recovery"
              << std::endl;
    std::cout << "Description:" << std::endl;
    std::cout << "Inform the system future reads and writes will never be earlier than the "
                 "specified timestamp."
              << std::endl;
}

int
oldest_timestamp::validate()
{
    std::cout << "validate" << std::endl;
    return (0);
}
