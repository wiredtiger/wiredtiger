#include "wt_internal.h"
#include <catch2/catch.hpp>

TEST_CASE("Bitstring macros: __bit_byte", "[bitstring]") {
    REQUIRE((__bit_byte(0)) == 0);
    REQUIRE((__bit_byte(1)) == 0);
    REQUIRE((__bit_byte(2)) == 0);
    REQUIRE((__bit_byte(3)) == 0);
    REQUIRE((__bit_byte(4)) == 0);
    REQUIRE((__bit_byte(5)) == 0);
    REQUIRE((__bit_byte(6)) == 0);
    REQUIRE((__bit_byte(7)) == 0);
    REQUIRE((__bit_byte(8)) == 1);
    REQUIRE((__bit_byte(9)) == 1);
    REQUIRE((__bit_byte(15)) == 1);
    REQUIRE((__bit_byte(16)) == 2);
}

TEST_CASE("Bitstring macros: __bit_mask", "[bitstring]") {
    REQUIRE((__bit_mask(0)) == 1);
    REQUIRE((__bit_mask(1)) == 2);
    REQUIRE((__bit_mask(2)) == 4);
    REQUIRE((__bit_mask(3)) == 8);
    REQUIRE((__bit_mask(4)) == 16);
    REQUIRE((__bit_mask(5)) == 32);
    REQUIRE((__bit_mask(6)) == 64);
    REQUIRE((__bit_mask(7)) == 128);
    REQUIRE((__bit_mask(8)) == 1);
    REQUIRE((__bit_mask(9)) == 2);
    REQUIRE((__bit_mask(10)) == 4);
    REQUIRE((__bit_mask(11)) == 8);
    REQUIRE((__bit_mask(12)) == 16);
    REQUIRE((__bit_mask(13)) == 32);
    REQUIRE((__bit_mask(14)) == 64);
    REQUIRE((__bit_mask(15)) == 128);
    REQUIRE((__bit_mask(16)) == 1);
    REQUIRE((__bit_mask(17)) == 2);
}

TEST_CASE("Bitstring macros: __bitstr_size", "[bitstring]") {
    REQUIRE((__bitstr_size(0)) == 0);
    REQUIRE((__bitstr_size(1)) == 1);
    REQUIRE((__bitstr_size(2)) == 1);
    REQUIRE((__bitstr_size(3)) == 1);
    REQUIRE((__bitstr_size(4)) == 1);
    REQUIRE((__bitstr_size(5)) == 1);
    REQUIRE((__bitstr_size(6)) == 1);
    REQUIRE((__bitstr_size(7)) == 1);
    REQUIRE((__bitstr_size(8)) == 1);
    REQUIRE((__bitstr_size(9)) == 2);
    REQUIRE((__bitstr_size(10)) == 2);
    REQUIRE((__bitstr_size(11)) == 2);
    REQUIRE((__bitstr_size(12)) == 2);
    REQUIRE((__bitstr_size(13)) == 2);
    REQUIRE((__bitstr_size(14)) == 2);
    REQUIRE((__bitstr_size(15)) == 2);
    REQUIRE((__bitstr_size(16)) == 2);
    REQUIRE((__bitstr_size(17)) == 3);
}

TEST_CASE("Bitstring functions: __bit_nset", "[bitstring]") {
    const int bitVectorSize = 8;
    std::vector<uint8_t> bitVector(bitVectorSize, 0);

    for (int i = 0; i < bitVectorSize; i++)
        REQUIRE(bitVector[i] == 0x00);

    SECTION("Simple test: set first two bytes") {
        __bit_nset(bitVector.data(), 0, 15);
        REQUIRE(bitVector[0] == 0xff);
        REQUIRE(bitVector[1] == 0xff);
        REQUIRE(bitVector[2] == 0x00);
        REQUIRE(bitVector[3] == 0x00);
        REQUIRE(bitVector[4] == 0x00);
        REQUIRE(bitVector[5] == 0x00);
        REQUIRE(bitVector[6] == 0x00);
        REQUIRE(bitVector[7] == 0x00);
    }

    SECTION("Simple test: set bytes 1 and 2 bytes") {
        __bit_nset(bitVector.data(), 8, 23);
        REQUIRE(bitVector[0] == 0x00);
        REQUIRE(bitVector[1] == 0xff);
        REQUIRE(bitVector[2] == 0xff);
        REQUIRE(bitVector[3] == 0x00);
        REQUIRE(bitVector[4] == 0x00);
        REQUIRE(bitVector[5] == 0x00);
        REQUIRE(bitVector[6] == 0x00);
        REQUIRE(bitVector[7] == 0x00);
    }

    SECTION("Simple test: set non byte-aligned bitVector") {
        __bit_nset(bitVector.data(), 9, 20);
        REQUIRE(bitVector[0] == 0x00);
        REQUIRE(bitVector[1] == 0xfe);
        REQUIRE(bitVector[2] == 0x1f);
        REQUIRE(bitVector[3] == 0x00);
        REQUIRE(bitVector[4] == 0x00);
        REQUIRE(bitVector[5] == 0x00);
        REQUIRE(bitVector[6] == 0x00);
        REQUIRE(bitVector[7] == 0x00);
    }

    SECTION("Simple test: first non byte-aligned bitVector") {
        __bit_nset(bitVector.data(), 0, 20);
        REQUIRE(bitVector[0] == 0xff);
        REQUIRE(bitVector[1] == 0xff);
        REQUIRE(bitVector[2] == 0x1f);
        REQUIRE(bitVector[3] == 0x00);
        REQUIRE(bitVector[4] == 0x00);
        REQUIRE(bitVector[5] == 0x00);
        REQUIRE(bitVector[6] == 0x00);
        REQUIRE(bitVector[7] == 0x00);
    }

    SECTION("Simple test: last non-aligned bitVector") {
        __bit_nset(bitVector.data(), 36, 63);
        REQUIRE(bitVector[0] == 0x00);
        REQUIRE(bitVector[1] == 0x00);
        REQUIRE(bitVector[2] == 0x00);
        REQUIRE(bitVector[3] == 0x00);
        REQUIRE(bitVector[4] == 0xf0);
        REQUIRE(bitVector[5] == 0xff);
        REQUIRE(bitVector[6] == 0xff);
        REQUIRE(bitVector[7] == 0xff);
    }
}





