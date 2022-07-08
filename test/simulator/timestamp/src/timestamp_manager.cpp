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

#include "timestamp_manager.h"
#include <iostream>

timestamp_manager::timestamp_manager() {}

/* Get an instance of connection_simulator class. */
timestamp_manager &
timestamp_manager::get_timestamp_manager()
{
    static timestamp_manager _timestamp_manager_instance;
    return (_timestamp_manager_instance);
}

bool timestamp_manager::validate_oldest_ts(int new_oldest_ts){
    connection_simulator *conn = &connection_simulator::get_connection();

    // Oldest timestamp can't move backward.
    if (new_oldest_ts <= conn->get_oldest_ts()){
        std::cout << "Oldest timestamp cannot move backwards." << std::endl;
        return false;
    }

    /*
     * The oldest and stable timestamps must always satisfy the condition that oldest <= stable.
     */
    if (new_oldest_ts > conn->get_stable_ts()){
        std::cout << "set_timestamp: oldest timestamp " << new_oldest_ts << "must not be later than stable timestamp " << conn->get_stable_ts() << "." << std::endl;
        return false;
    }

    return true;
}