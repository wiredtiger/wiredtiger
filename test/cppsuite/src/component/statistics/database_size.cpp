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

#include "database_size.h"

#include "src/common/logger.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {

static std::string
ConvertCollectionNameToFilename(const std::string &collectionName)
{
    /* Strip out the URI prefix. */
    const size_t colonPos = collectionName.find(':');
    testutil_assert(colonPos != std::string::npos);
    const auto strippedName = collectionName.substr(colonPos + 1);

    /* Now add the directory and file extension. */
    return (std::string(DEFAULT_DIR) + "/" + strippedName + ".wt");
}

DatabaseSize::DatabaseSize(Configuration &config, const std::string &name, database &database)
    : Statistics(config, name, -1), _database(database)
{
#ifdef _WIN32
    Logger::LogMessage("Database size checking is not implemented on Windows", LOG_ERROR);
#endif
}

void
DatabaseSize::Check(scoped_cursor &)
{
#ifndef _WIN32
    const auto filenames = GetFilenames();
    size_t databaseSize = GetDatabaseSize();
    Logger::LogMessage(
      LOG_TRACE, "Current database size is " + std::to_string(databaseSize) + " bytes");

    if (databaseSize > max) {
        const std::string error =
          "MetricsMonitor: Database size limit exceeded during test! Limit: " +
          std::to_string(max) + " db size: " + std::to_string(databaseSize);
        testutil_die(-1, error.c_str());
    }
#endif
}

std::string
DatabaseSize::GetValueString(scoped_cursor &)
{
    return std::to_string(GetDatabaseSize());
}

size_t
DatabaseSize::GetDatabaseSize() const
{
    const auto fileNames = GetFilenames();
    size_t databaseSize = 0;

    for (const auto &name : fileNames) {
        struct stat sb;
        if (stat(name.c_str(), &sb) == 0) {
            databaseSize += sb.st_size;
            Logger::LogMessage(LOG_TRACE, name + " was " + std::to_string(sb.st_size) + " bytes");
        } else
            /* The only good reason for this to fail is if the file hasn't been created yet. */
            testutil_assert(errno == ENOENT);
    }

    return databaseSize;
}

const std::vector<std::string>
DatabaseSize::GetFilenames() const
{
    std::vector<std::string> fileNames;
    for (const auto &name : _database.get_collection_names())
        fileNames.push_back(ConvertCollectionNameToFilename(name));

    /* Add WiredTiger internal tables. */
    fileNames.push_back(std::string(DEFAULT_DIR) + "/" + WT_HS_FILE);
    fileNames.push_back(std::string(DEFAULT_DIR) + "/" + WT_METAFILE);

    return (fileNames);
}
} // namespace test_harness
