#include <cstdint>
#include <cstdlib>
#include <iostream>

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

uint32_t factorial(uint32_t num) {
    return num > 1 ? factorial(num - 1) * num : 1;
}

TEST_CASE("Factorials are computed", "[factorial]") {
    REQUIRE(factorial(0) == 1);
    REQUIRE(factorial(1) == 1);
    REQUIRE(factorial(2) == 2);
    REQUIRE(factorial(3) == 6);
    REQUIRE(factorial(10) == 3628800);
}

int main(int argc, char** argv) {
    std::cout << "Hello, world!" << std::endl;

    std::cout << "factorial(2)=" << factorial(2) << std::endl;

    int result = Catch::Session().run(argc, argv);

    return EXIT_SUCCESS;
}
