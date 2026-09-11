/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_header_report]: block_read.c
 * This file unit tests the diagnostic reported when a block fails its checksum. The classification
 * that matters is whether the size the block declares for itself agrees with the size that was
 * asked for: agreement means the expected block arrived damaged, disagreement means a different
 * block arrived instead.
 */

#include <catch2/catch.hpp>
#include <cstring>
#include <string>
#include <vector>

#include "wt_internal.h"

namespace {

/*
 * Build a block image whose page header holds the supplied fields. The image is in on-disk order,
 * which is what the reporting code is handed.
 */
std::vector<uint8_t>
build_image(uint32_t mem_size, uint64_t write_gen, uint32_t entries, uint8_t type, uint8_t flags,
  uint8_t version)
{
    std::vector<uint8_t> image(WT_BLOCK_HEADER_BYTE_SIZE, 0);
    WT_PAGE_HEADER dsk;

    memset(&dsk, 0, sizeof(dsk));
    dsk.mem_size = mem_size;
    dsk.write_gen = write_gen;
    dsk.u.entries = entries;
    dsk.type = type;
    dsk.flags = flags;
    dsk.version = version;

    /* The byte-swap is its own inverse, so this converts host order to on-disk order. */
    __wt_page_header_byteswap(&dsk);
    memcpy(image.data(), &dsk, sizeof(dsk));

    return image;
}

/* The block header reaches the reporting code already byte-swapped, so it needs no conversion. */
std::string
report(const std::vector<uint8_t> &image, uint32_t disk_size, uint8_t flags, uint32_t size,
  size_t dst_size = 512)
{
    WT_BLOCK_HEADER blk;
    std::vector<char> dst(dst_size, 'x');

    memset(&blk, 0, sizeof(blk));
    blk.disk_size = disk_size;
    blk.flags = flags;

    __ut_block_header_report(image.data(), &blk, size, dst.data(), dst.size());

    /* The formatting is not allowed to run off the end of the buffer. */
    REQUIRE(memchr(dst.data(), '\0', dst.size()) != nullptr);
    return std::string(dst.data());
}

} // namespace

TEST_CASE("Block header report: size classification", "[block_header_report]")
{
    SECTION("A block declaring the size that was asked for is the expected block, damaged")
    {
        auto image =
          build_image(8192, 42, 71, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("HEADER_SIZE_MATCH") == 0);
        CHECK(s.find("HEADER_SIZE_MISMATCH") == std::string::npos);
        CHECK(s.find("disk_size 4096") != std::string::npos);
        CHECK(s.find("requested size 4096") != std::string::npos);
    }

    SECTION("A block declaring a different size is a different block")
    {
        auto image =
          build_image(24576, 99, 130, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 12288);

        CHECK(s.find("HEADER_SIZE_MISMATCH") == 0);
        CHECK(s.find("disk_size 4096") != std::string::npos);
        CHECK(s.find("requested size 12288") != std::string::npos);
    }

    SECTION("The decoded header fields are reported")
    {
        auto image =
          build_image(8192, 42, 71, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("mem_size 8192") != std::string::npos);
        CHECK(s.find("write_gen 42") != std::string::npos);
        CHECK(s.find("entries 71") != std::string::npos);
        CHECK(s.find("WT_PAGE_ROW_LEAF") != std::string::npos);
        CHECK(s.find("version 1") != std::string::npos);
    }

    SECTION("A page type outside the known set is named rather than dropped")
    {
        auto image = build_image(8192, 42, 71, 0xdb, 0, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("type 219 (PAGE_TYPE_INVALID)") != std::string::npos);
    }
}

TEST_CASE("Block header report: in-memory size plausibility", "[block_header_report]")
{
    SECTION("A compressed block no larger in memory than on disk is called out")
    {
        auto image =
          build_image(4096, 42, 71, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("IMPLAUSIBLE_MEM_SIZE") != std::string::npos);
    }

    SECTION("An uncompressed block no larger in memory than on disk is not called out")
    {
        auto image = build_image(4096, 42, 71, WT_PAGE_ROW_LEAF, 0, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("IMPLAUSIBLE_MEM_SIZE") == std::string::npos);
    }

    SECTION("A compressed block larger in memory than on disk is not called out")
    {
        auto image =
          build_image(8192, 42, 71, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 4096);

        CHECK(s.find("IMPLAUSIBLE_MEM_SIZE") == std::string::npos);
    }
}

TEST_CASE("Block header report: extreme values", "[block_header_report]")
{
    SECTION("Wild sizes are reported, not acted on")
    {
        auto image = build_image(
          UINT32_MAX, UINT64_MAX, UINT32_MAX, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, 0xff);
        std::string s = report(image, UINT32_MAX, 0xff, 4096);

        CHECK(s.find("HEADER_SIZE_MISMATCH") == 0);
        CHECK(s.find("mem_size 4294967295") != std::string::npos);
        CHECK(s.find("write_gen 18446744073709551615") != std::string::npos);
    }

    SECTION("A buffer too small to hold the report is truncated, not overrun")
    {
        auto image =
          build_image(8192, 42, 71, WT_PAGE_ROW_LEAF, WT_PAGE_COMPRESSED, WT_PAGE_VERSION_TS);
        std::string s = report(image, 4096, WT_BLOCK_DATA_CKSUM, 12288, 8);

        CHECK(s == "HEADER_");
    }
}
