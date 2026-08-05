/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include <cstdarg>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

#include "wt_internal.h"
#include "../../wrappers/mock_session.h"

namespace {

constexpr auto filename = "test.wt";
constexpr auto file_uri = "file:test.wt";
constexpr auto table_uri = "table:test";
constexpr auto stable_uri = "file:test.wt_stable";

class mock_metadata_cursor {
public:
    using response = std::variant<std::string, int>;

    mock_metadata_cursor()
    {
        _cursor.lang_private = this;
        _cursor.set_key = set_key;
        _cursor.search = search;
        _cursor.get_value = get_value;
        _cursor.reset = reset;
    }

    [[nodiscard]] WT_CURSOR &
    cursor()
    {
        return _cursor;
    }

    void
    insert_metadata(std::string_view uri, std::string_view value)
    {
        _responses.insert_or_assign(std::string(uri), response{std::string(value)});
    }

    void
    insert_metadata_error(std::string_view uri, int error)
    {
        _responses.insert_or_assign(std::string(uri), response{error});
    }

private:
    static mock_metadata_cursor &
    self(WT_CURSOR *cursor)
    {
        return *static_cast<mock_metadata_cursor *>(cursor->lang_private);
    }

    static void
    set_key(WT_CURSOR *cursor, ...)
    {
        va_list ap;
        va_start(ap, cursor);
        self(cursor)._key = va_arg(ap, const char *);
        va_end(ap);
    }

    [[nodiscard]] const response *
    find_response() const
    {
        const auto it = _responses.find(_key);
        if (it == _responses.end())
            return nullptr;

        return &it->second;
    }

    static int
    search(WT_CURSOR *cursor)
    {
        const auto *response = self(cursor).find_response();
        if (response == nullptr)
            return WT_NOTFOUND;

        const auto *error = std::get_if<int>(response);
        return error == nullptr ? 0 : *error;
    }

    static int
    get_value(WT_CURSOR *cursor, ...)
    {
        const auto *response = self(cursor).find_response();
        if (response == nullptr)
            return WT_NOTFOUND;

        const auto *value = std::get_if<std::string>(response);
        if (value == nullptr)
            return WT_NOTFOUND;

        va_list ap;
        va_start(ap, cursor);
        *va_arg(ap, const char **) = value->c_str();
        va_end(ap);
        return 0;
    }

    static int
    reset(WT_CURSOR *)
    {
        return 0;
    }

    WT_CURSOR _cursor{};

    std::unordered_map<std::string, response> _responses;
    std::string _key;
};

int
fs_exist_default(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *existp)
{
    *existp = true;
    return 0;
}

int
fs_size_default(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *sizep)
{
    *sizep = 0;
    return 0;
}

class curstat_size_fixture {
public:
    curstat_size_fixture() : _mock(mock_session::build_test_mock_session())
    {
        const auto connection = _mock->get_mock_connection();
        auto *conn = connection->get_wt_connection_impl();
        auto *session_impl = session();

        REQUIRE(connection->setup_block_manager(session_impl) == 0);

        // Metadata searches need both session and shared transaction state.
        REQUIRE(__wt_calloc_one(session_impl, &_txn) == 0);
        conn->txn_global.txn_shared_list = &_txn_shared;
        session_impl->txn = _txn;

        conn->file_system->fs_exist = fs_exist_default;
        conn->file_system->fs_size = fs_size_default;

        // Use the mock cursor for metadata lookups.
        session_impl->meta_cursor = &_metadata_cursor.cursor();
    }

    ~curstat_size_fixture()
    {
        auto *session_impl = session();
        auto *conn = S2C(session_impl);

        // Detach test state before the mock session is destroyed.
        session_impl->meta_cursor = nullptr;
        session_impl->txn = nullptr;
        conn->txn_global.txn_shared_list = nullptr;
        __wt_free(session_impl, _txn);

        // Reset optional disaggregated configuration.
        conn->disaggregated_storage.page_log_meta = nullptr;
        __wt_conn_config_discard(session_impl);
    }

    void
    enable_disaggregated_storage()
    {
        auto *session_impl = session();

        // File classification needs the default metadata configuration.
        REQUIRE(__wt_conn_config_init(session_impl) == 0);

        // A metadata page log handle marks the connection as disaggregated.
        S2C(session_impl)->disaggregated_storage.page_log_meta = &_page_log_handle;
    }

    [[nodiscard]] WT_FILE_SYSTEM &
    file_system()
    {
        return *S2C(session())->file_system;
    }

    [[nodiscard]] mock_metadata_cursor &
    metadata_cursor()
    {
        return _metadata_cursor;
    }

    [[nodiscard]] WT_SESSION_IMPL *
    session() const
    {
        return _mock->get_wt_session_impl();
    }

private:
    std::shared_ptr<mock_session> _mock;
    mock_metadata_cursor _metadata_cursor;
    WT_TXN *_txn = nullptr;
    WT_TXN_SHARED _txn_shared{};
    WT_PAGE_LOG_HANDLE _page_log_handle{};
};

} // namespace

SCENARIO_METHOD(curstat_size_fixture, "local size propagates a file system error", "[curstat_size]")
{
    GIVEN("a file system that returns an existence-check error")
    {
        constexpr auto expected_error = EIO;
        file_system().fs_exist = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *) {
            return expected_error;
        };

        WHEN("the local size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_size_local(session(), filename, &was_fast, &size);

            THEN("it returns the file system error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "local size reports a missing file", "[curstat_size]")
{
    GIVEN("a file system that reports the file does not exist")
    {
        file_system().fs_exist = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *existp) {
            *existp = false;
            return 0;
        };

        WHEN("the local size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_size_local(session(), filename, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path did not resolve the size")
            {
                REQUIRE_FALSE(was_fast);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "local size propagates a size error", "[curstat_size]")
{
    GIVEN("a file system that returns a size-check error")
    {
        constexpr auto expected_error = EIO;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *) {
            return expected_error;
        };

        WHEN("the local size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_size_local(session(), filename, &was_fast, &size);

            THEN("it returns the size error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(
  curstat_size_fixture, "local size treats a concurrent removal as missing", "[curstat_size]")
{
    GIVEN("a file system whose size check returns ENOENT")
    {
        constexpr auto expected_error = ENOENT;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *) {
            return expected_error;
        };

        WHEN("the local size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_size_local(session(), filename, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path did not resolve the size")
            {
                REQUIRE_FALSE(was_fast);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "local size reports a valid size", "[curstat_size]")
{
    GIVEN("a file system that reports an existing file with a valid size")
    {
        constexpr wt_off_t expected_size = 5678;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *sizep) {
            *sizep = expected_size;
            return 0;
        };

        WHEN("the local size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_size_local(session(), filename, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the file size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "shared size propagates a checkpoint error", "[curstat_size]")
{
    GIVEN("shared-file metadata with a checkpoint missing its order")
    {
        constexpr auto expected_error = WT_ERROR;
        constexpr auto config =
          "checkpoint=(WiredTigerCheckpoint.1=(addr=\"\",order=,time=1,size=0,write_gen=1))";

        WHEN("the shared size is requested")
        {
            int64_t size = 0;
            const auto result = __ut_curstat_size_shared(session(), config, &size);

            THEN("it returns the checkpoint error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "shared size reports zero when there is no checkpoint entry",
  "[curstat_size]")
{
    GIVEN("shared-file metadata with an empty checkpoint list")
    {
        constexpr auto config = "checkpoint=()";

        WHEN("the shared size is requested")
        {
            int64_t size = 1;
            const auto result = __ut_curstat_size_shared(session(), config, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it reports a zero size")
            {
                REQUIRE(size == 0);
            }
        }
    }
}

SCENARIO_METHOD(
  curstat_size_fixture, "shared size reports a zero-sized checkpoint", "[curstat_size]")
{
    GIVEN("shared-file metadata with a zero-sized checkpoint")
    {
        constexpr auto config =
          "checkpoint=(WiredTigerCheckpoint.1=(addr=\"\",order=1,time=1,size=0,write_gen=1))";

        WHEN("the shared size is requested")
        {
            int64_t size = 1;
            const auto result = __ut_curstat_size_shared(session(), config, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it reports a zero size")
            {
                REQUIRE(size == 0);
            }
        }
    }
}

SCENARIO_METHOD(
  curstat_size_fixture, "shared size reports a nonzero checkpoint size", "[curstat_size]")
{
    GIVEN("shared-file metadata with a nonzero checkpoint size")
    {
        constexpr int64_t expected_size = 5678;
        constexpr auto config =
          "checkpoint=(WiredTigerCheckpoint.1=(addr=\"\",order=1,time=1,size=5678,write_gen=1))";

        WHEN("the shared size is requested")
        {
            int64_t size = 0;
            const auto result = __ut_curstat_size_shared(session(), config, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it reports the checkpoint size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size propagates a metadata search error",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a disaggregated connection whose metadata search returns an error")
    {
        enable_disaggregated_storage();

        constexpr auto expected_error = EIO;
        metadata_cursor().insert_metadata_error(file_uri, expected_error);

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns the metadata search error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size propagates a block manager classification error",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a disaggregated connection with invalid block manager metadata")
    {
        enable_disaggregated_storage();

        constexpr auto expected_error = EINVAL;
        constexpr auto file_config = "block_manager=(";
        metadata_cursor().insert_metadata(file_uri, file_config);

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns the block manager classification error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size propagates an error from shared sizing",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a disaggregated connection with invalid shared-file checkpoint metadata")
    {
        enable_disaggregated_storage();

        constexpr auto file_config =
          "block_manager=disagg,"
          "checkpoint=(WiredTigerCheckpoint.1=(addr=\"\",order=))";
        metadata_cursor().insert_metadata(file_uri, file_config);

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns the shared sizing error")
            {
                REQUIRE(result == WT_ERROR);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size retrieves the shared checkpoint size",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a disaggregated connection with shared-file metadata")
    {
        enable_disaggregated_storage();

        constexpr int64_t expected_size = 5678;
        constexpr auto file_config =
          "block_manager=disagg,"
          "checkpoint=(WiredTigerCheckpoint.1=("
          "addr=\"\",order=1,time=1,size=5678,write_gen=1))";
        metadata_cursor().insert_metadata(file_uri, file_config);

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the shared checkpoint size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size propagates an error from local sizing",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a local connection whose file system size check returns an error")
    {
        constexpr auto expected_error = EIO;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *) {
            return expected_error;
        };

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns the local sizing error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "file size retrieves the local file size",
  "[curstat_size][curstat_file_size]")
{
    GIVEN("a local connection with an existing file")
    {
        constexpr wt_off_t expected_size = 4321;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *sizep) {
            *sizep = expected_size;
            return 0;
        };

        WHEN("the file size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_file_size(session(), file_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the local file size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size reports missing table metadata",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("no metadata for the table")
    {
        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns not found")
            {
                REQUIRE(result == WT_NOTFOUND);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size propagates a metadata search error",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a table metadata search that returns an error")
    {
        constexpr auto expected_error = EIO;
        metadata_cursor().insert_metadata_error(table_uri, expected_error);

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the metadata search error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size propagates a columns lookup error",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("table metadata without a columns configuration")
    {
        metadata_cursor().insert_metadata(table_uri, "key_format=S");

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the columns lookup error")
            {
                REQUIRE(result == WT_NOTFOUND);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size propagates a simple-table classification error",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("table metadata with malformed columns")
    {
        metadata_cursor().insert_metadata(table_uri, "columns=\"(\"");

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the simple-table classification error")
            {
                REQUIRE(result == EINVAL);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size skips the fast path for a non-simple table",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("table metadata with named columns")
    {
        metadata_cursor().insert_metadata(table_uri, "columns=(key,value)");

        WHEN("the table size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path did not resolve the size")
            {
                REQUIRE_FALSE(was_fast);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture,
  "table size propagates an error from the backing file size lookup",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table whose backing file size lookup returns an error")
    {
        metadata_cursor().insert_metadata(table_uri, "columns=()");

        constexpr auto expected_error = EIO;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *) {
            return expected_error;
        };

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the backing file size lookup error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size retrieves its backing file size",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table backed by a local file")
    {
        metadata_cursor().insert_metadata(table_uri, "columns=()");

        constexpr wt_off_t expected_size = 4321;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *sizep) {
            *sizep = expected_size;
            return 0;
        };

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the backing file size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size propagates a stable metadata search error",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table whose stable metadata search returns an error")
    {
        enable_disaggregated_storage();

        metadata_cursor().insert_metadata(table_uri, "columns=()");

        constexpr auto expected_error = EIO;
        metadata_cursor().insert_metadata_error(stable_uri, expected_error);

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the stable metadata search error")
            {
                REQUIRE(result == expected_error);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size propagates an error from shared sizing",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table with invalid stable checkpoint metadata")
    {
        enable_disaggregated_storage();

        metadata_cursor().insert_metadata(table_uri, "columns=()");

        constexpr auto stable_config = "checkpoint=(WiredTigerCheckpoint.1=(addr=\"\",order=))";
        metadata_cursor().insert_metadata(stable_uri, stable_config);

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns the shared sizing error")
            {
                REQUIRE(result == WT_ERROR);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture, "table size retrieves the shared checkpoint size",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table with stable checkpoint metadata")
    {
        enable_disaggregated_storage();

        metadata_cursor().insert_metadata(table_uri, "columns=()");

        constexpr int64_t expected_size = 5678;
        constexpr auto stable_config =
          "checkpoint=(WiredTigerCheckpoint.1=("
          "addr=\"\",order=1,time=1,size=5678,write_gen=1))";

        metadata_cursor().insert_metadata(stable_uri, stable_config);

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the shared checkpoint size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture,
  "table size defers to the slow path when stable and local file metadata are missing",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table without stable or local file metadata")
    {
        enable_disaggregated_storage();

        metadata_cursor().insert_metadata(table_uri, "columns=()");

        WHEN("the table size is requested")
        {
            bool was_fast = true;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path did not resolve the size")
            {
                REQUIRE_FALSE(was_fast);
            }
        }
    }
}

SCENARIO_METHOD(curstat_size_fixture,
  "table size falls back to its local backing file when stable metadata is missing",
  "[curstat_size][curstat_table_size]")
{
    GIVEN("a simple table without stable metadata")
    {
        enable_disaggregated_storage();

        metadata_cursor().insert_metadata(table_uri, "columns=()");

        // Mark the fallback file as local on the disaggregated connection.
        metadata_cursor().insert_metadata(file_uri, "block_manager=default");

        constexpr wt_off_t expected_size = 4321;
        file_system().fs_size = [](WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *sizep) {
            *sizep = expected_size;
            return 0;
        };

        WHEN("the table size is requested")
        {
            bool was_fast = false;
            int64_t size = 0;
            const auto result = __ut_curstat_table_size(session(), table_uri, &was_fast, &size);

            THEN("it returns zero")
            {
                REQUIRE(result == 0);
            }

            AND_THEN("it indicates that the fast path resolved the size")
            {
                REQUIRE(was_fast);
            }

            AND_THEN("it reports the local backing file size")
            {
                REQUIRE(size == expected_size);
            }
        }
    }
}
