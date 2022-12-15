/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "utils.h"
#include "wiredtiger.h"
#include "wrappers/connection_wrapper.h"
#include "wt_internal.h"

// #define CALL_WT_ERR_MACRO(session, assert_should_pass, ...) \
//     WT_ERR_ASSERT(session, assert_should_pass, -1, "boop err"); \
//     ret = 14; \
//     if (0){ \
// err: \
//         ret = 13; \
//     }\

// #define CALL_WT_RET_MACRO(session, assert_should_pass, ...) \
//     WT_RET_ASSERT(session, assert_should_pass, -1, "boop ret"); \
    // ret = 14; 


/* Assert that a WT assertion fired with the expected message, then clear the flag and message. */
void
expect_assertion(WT_SESSION_IMPL *session, std::string expected_message)
{
    REQUIRE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg) == expected_message);

    // Clear the assertion flag and message for the next test step
    session->unittest_assert_hit = false;
    memset(session->unittest_assert_msg, 0, WT_SESSION_UNITTEST_BUF_LEN);
}

void
expect_no_assertion(WT_SESSION_IMPL *session)
{
    REQUIRE_FALSE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg).empty());
}


int
call_wt_ret(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    if(assert_should_pass) {
        WT_RET_ASSERT(session, 1 == 1, -1, "WT_RET raised assert");
    } else {
        WT_RET_ASSERT(session, 1 == 2, -1, "WT_RET raised assert");
    }
    return 14;
}

    
int
call_wt_err(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    WT_DECL_RET;

    if(assert_should_pass) {
        WT_ERR_ASSERT(session, 1 == 1, -1, "WT_ERR raised assert");
    } else {
        WT_RET_ASSERT(session, 1 == 2, -1, "WT_ERR raised assert");
    }
    ret = 14;

    if (0){
err:
        ret = 13;
    }
    return ret;
}

int
call_wt_panic(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    WT_DECL_RET;

    if(assert_should_pass) {
        WT_RET_PANIC_ASSERT(session, 1 == 1, -1, "WT_PANIC raised assert");
    } else {
        WT_RET_PANIC_ASSERT(session, 1 == 2, -1, "WT_PANIC raised assert");
    }
    ret = 14;

    return ret;
}

int
all_asserts_abort(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    int ret = 0;

    WT_ASSERT(session, 1 == 2);
    expect_assertion(session,"Assertion '1 == 2' failed: Expression returned false");

    WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
    expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

    REQUIRE(call_wt_ret(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_RET raised assert");

    REQUIRE(call_wt_err(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_ERR raised assert");

    REQUIRE(call_wt_panic(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_PANIC raised assert");

    return ret;
}

int
diagnostic_asserts_off(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    int ret = 0;

    // WT_ASSERT does nothing.
    WT_ASSERT(session, 1 == 2);
    expect_no_assertion(session);

    // WT_ASSERT_ALWAYS aborts.
    WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
    expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

    // Throws and returns respective error messages.
    REQUIRE(call_wt_err(session, 1 == 2) == -1);
    REQUIRE(call_wt_ret(session, 1 == 2) == -1);
    REQUIRE(call_wt_panic(session, 1 == 2) ==  -31804);

    return ret;

}

// TEST_CASE("Simple implementation of unit testing WT_ASSERT_ALWAYS", "[assertions]")
// {
//     ConnectionWrapper conn(DB_HOME);
//     WT_SESSION_IMPL *session = conn.createSession();

//     // Check that the new session has set up our test fields correctly.
//     expect_no_assertion(session);

//     SECTION("Basic WT_ASSERT_ALWAYS tests")
//     {
//         WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
//         expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

//         WT_ASSERT_ALWAYS(session, 1 == 1, "Values are not equal!");
//         expect_no_assertion(session);
//     }
// }

// TEST_CASE("0 ", "[assertions]")
// {    
//     ConnectionWrapper conn(DB_HOME);

//     WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=on");

//     WT_ASSERT(session, 1 == 1);
//     REQUIRE(call_wt_err(session, 1 == 1) == 14);
//         // All five asserts should abort as expected.
//     // REQUIRE(all_asserts_abort(session, 1 == 1) == 0);
//     // // Testing debug asserts with diag asserts turned on
//     // WT_ERR_ASSERT(session1, 1 == 2, "ERR ASSERT: Values are not equal!");
//     // expect_assertion(session1, "ERR ASSERT: Values are not equal!");
//     // WT_RET_ASSERT(session1, 1 == 2, "RET ASSERT: Values are not equal!");
//     // expect_assertion(session1, "RET ASSERT: Values are not equal!");
//     // WT_RET_PANIC_ASSERT(session1, 1 == 2, "RET ASSERT: Values are not equal!");
//     // expect_assertion(session1, "RET PANIC ASSERT: Values are not equal!");

//     // // Testing debug asserts with diag asserts turned off
//     // WT_ERR_ASSERT(session2, 1 == 2, "Values are not equal!");
//     // expect_no_assertion(session2);
//     // WT_RET_ASSERT(session2, 1 == 2, "RET ASSERT: Values are not equal!");
//     // expect_no_assertion(session2);
//     // WT_RET_PANIC_ASSERT(session2, 1 == 2, "RET ASSERT: Values are not equal!");
//     // expect_no_assertion(session2);
// }

TEST_CASE("1a", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);

    // Configure the session to inherit diagnostic_asserts from connection.
    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=on");

    // All five asserts should abort as expected.
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);
}

TEST_CASE("1b", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);

    // Configure the session to inherit diagnostic_asserts from connection.
    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=off");
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("1c", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();

    // Configure the session to inherit diagnostic_asserts from connection.
    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=connection");

    // Reconfigure the connection with diagnostic_asserts = on.
    connection->reconfigure(connection, "diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);

    // Reconfigure the connection with diagnostic_asserts = off.  
    connection->reconfigure(connection, "diagnostic_asserts=off");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);

    // Reconfigure the connection with diagnostic_asserts = default.  
    connection->reconfigure(connection, "");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("1d", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    // Reconfigure the connection with diagnostic_asserts = on.
    connection->reconfigure(connection, "diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);

    // Reconfigure the connection with diagnostic_asserts = off.  
    connection->reconfigure(connection, "diagnostic_asserts=off");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);

    // Reconfigure the connection with diagnostic_asserts = default.  
    connection->reconfigure(connection, "");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("2a", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=connection");

    connection->reconfigure(connection, "diagnostic_asserts=off");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);

    connection->reconfigure(connection, "diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);
}


TEST_CASE("2aii", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=connection");

    connection->reconfigure(connection, "diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);

    connection->reconfigure(connection, "diagnostic_asserts=off");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("2b", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session1 = conn.createSession("diagnostic_asserts=off");
    WT_SESSION_IMPL *session2 = conn.createSession("diagnostic_asserts=on");
    // WT_SESSION_IMPL *session3 = conn.createSession("");

    auto sess1 = (WT_SESSION *) session1;
    auto sess2 = (WT_SESSION *) session2;
    // REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session1)) == true);
    // REQUIRE(all_asserts_abort(session1, 1 == 2) == 0);

    // REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session2)) == false);
    // REQUIRE(diagnostic_asserts_off(session2, 1 == 2) == 0);

    // REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session3)) == false);
    // REQUIRE(diagnostic_asserts_off(session3, 1 == 2) == 0);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session1)) == false);
    REQUIRE(diagnostic_asserts_off(session1, 1 == 2) == 0);

    sess1->reconfigure(sess1,"diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session1)) == true);
    REQUIRE(all_asserts_abort(session1, 1 == 2) == 0);

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session2)) == true);
    REQUIRE(all_asserts_abort(session2, 1 == 2) == 0);

    sess2->reconfigure(sess2,"diagnostic_asserts=off");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session2)) == false);
    REQUIRE(diagnostic_asserts_off(session2, 1 == 2) == 0);


}

TEST_CASE("2c", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    
    connection->reconfigure(connection, "diagnostic_asserts=off");

    WT_SESSION_IMPL *session = conn.createSession("diagnostic_asserts=on");
    auto sess = (WT_SESSION *) session;
    
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);

    sess->reconfigure(sess,"diagnostic_asserts=connection");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(diagnostic_asserts_off(session, 1 == 2) == 0);

    sess->reconfigure(sess,"diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(all_asserts_abort(session, 1 == 2) == 0);
}

// TEST_CASE("Set diagnostic asserts on session via connection", "[assertions]")
// {
//     ConnectionWrapper conn(DB_HOME);

//     WT_SESSION_IMPL *session1 = conn.createSession("diagnostic_asserts=on");
//     // REPLACE WITH: REQUIRE(diagnostic_asserts_enabled(session1));
//     REQUIRE(session1->diagnostic_asserts_level == DIAG_ASSERTS_ON); 

//     // This behaviour is disabled if compiled with DHAVE_DIAGNOSTICS = 1?
//     // WT_SESSION_IMPL *session2 = conn.createSession("diagnostic_asserts=off");
//     // REPLACE WITH: REQUIRE(diagnostic_asserts_enabled(session1));

//     // FIXME: WT-10043 Change to DIAG_ASSERTS_ON 
//     // REQUIRE(session2->diagnostic_asserts_level == DIAG_ASSERTS_OFF);   
//     // REQUIRE(session2->diagnostic_asserts_level == DIAG_ASSERTS_OFF); 
// }

TEST_CASE("3", "[assertions]")
{    
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session1 = conn.createSession("diagnostic_asserts=on");
    WT_SESSION_IMPL *session2 = conn.createSession("diagnostic_asserts=on");

    auto sess1 = (WT_SESSION *) session1;
          
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session1)) == true);
    REQUIRE(all_asserts_abort(session1, 1 == 2) == 0);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session2)) == true);
    REQUIRE(all_asserts_abort(session2, 1 == 2) == 0); 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ;
    sess1->reconfigure(sess1,"diagnostic_asserts=off");

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session1)) == false);
    REQUIRE(diagnostic_asserts_off(session1, 1 == 2) == 0);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session2)) == true);
    REQUIRE(all_asserts_abort(session2, 1 == 2) == 0); 
}

