#include <catch2/catch.hpp>
#include "wt_internal.h"

TEST_CASE("Bitstring macros: __bit_byte", "") {
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

TEST_CASE("Bitstring macros: __bit_mask", "") {
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

TEST_CASE("Bitstring macros: __bitstr_size", "") {
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

TEST_CASE("Bitstring functions: __bit_nset", "") {
    const int bit_vector_size = 8;
    std::vector<uint8_t> bits(bit_vector_size, 0);

    REQUIRE(bits[0] == 0x00);
    REQUIRE(bits[1] == 0x00);
    REQUIRE(bits[2] == 0x00);
    REQUIRE(bits[3] == 0x00);
    REQUIRE(bits[4] == 0x00);
    REQUIRE(bits[5] == 0x00);
    REQUIRE(bits[6] == 0x00);
    REQUIRE(bits[7] == 0x00);

    SECTION("Simple test: set first two bytes") {
        __bit_nset(bits.data(), 0, 15);
        REQUIRE(bits[0] == 0xff);
        REQUIRE(bits[1] == 0xff);
        REQUIRE(bits[2] == 0x00);
        REQUIRE(bits[3] == 0x00);
        REQUIRE(bits[4] == 0x00);
        REQUIRE(bits[5] == 0x00);
        REQUIRE(bits[6] == 0x00);
        REQUIRE(bits[7] == 0x00);
    }

    SECTION("Simple test: set bytes 1 and 2 bytes") {
        __bit_nset(bits.data(), 8, 23);
        REQUIRE(bits[0] == 0x00);
        REQUIRE(bits[1] == 0xff);
        REQUIRE(bits[2] == 0xff);
        REQUIRE(bits[3] == 0x00);
        REQUIRE(bits[4] == 0x00);
        REQUIRE(bits[5] == 0x00);
        REQUIRE(bits[6] == 0x00);
        REQUIRE(bits[7] == 0x00);
    }

    SECTION("Simple test: set non byte-aligned bits") {
        __bit_nset(bits.data(), 9, 20);
        REQUIRE(bits[0] == 0x00);
        REQUIRE(bits[1] == 0xfe);
        REQUIRE(bits[2] == 0x1f);
        REQUIRE(bits[3] == 0x00);
        REQUIRE(bits[4] == 0x00);
        REQUIRE(bits[5] == 0x00);
        REQUIRE(bits[6] == 0x00);
        REQUIRE(bits[7] == 0x00);
    }

    SECTION("Simple test: first non byte-aligned bits") {
        __bit_nset(bits.data(), 0, 20);
        REQUIRE(bits[0] == 0xff);
        REQUIRE(bits[1] == 0xff);
        REQUIRE(bits[2] == 0x1f);
        REQUIRE(bits[3] == 0x00);
        REQUIRE(bits[4] == 0x00);
        REQUIRE(bits[5] == 0x00);
        REQUIRE(bits[6] == 0x00);
        REQUIRE(bits[7] == 0x00);
    }

    SECTION("Simple test: last non-aligned bits") {
        __bit_nset(bits.data(), 36, 63);
        REQUIRE(bits[0] == 0x00);
        REQUIRE(bits[1] == 0x00);
        REQUIRE(bits[2] == 0x00);
        REQUIRE(bits[3] == 0x00);
        REQUIRE(bits[4] == 0xf0);
        REQUIRE(bits[5] == 0xff);
        REQUIRE(bits[6] == 0xff);
        REQUIRE(bits[7] == 0xff);
    }
}


TEST_CASE("Block helper: __wt_rduppo2", "") {
    // Expected valid calls
    REQUIRE(__wt_rduppo2(1, 32) == 32);
    REQUIRE(__wt_rduppo2(24, 32) == 32);
    REQUIRE(__wt_rduppo2(42, 32) == 64);
    REQUIRE(__wt_rduppo2(42, 128) == 128);

    // Expected invalid calls
    REQUIRE(__wt_rduppo2(1, 42) == 0);
    REQUIRE(__wt_rduppo2(102, 42) == 0);
}



