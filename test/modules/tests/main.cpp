/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

int
main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
