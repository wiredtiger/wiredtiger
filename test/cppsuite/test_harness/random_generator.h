/* Include guard. */
#ifndef RANDOM_GENERATOR_H
#define RANDOM_GENERATOR_H

#include <algorithm>
#include <cstddef>
#include <random>
#include <string>

namespace test_harness {

/* Generate random values. */
class random_generator {

    static random_generator *instance;
    std::uniform_int_distribution<> distribution;
    std::mt19937 _generator;
    std::random_device _random_device;

    const std::string CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    random_generator()
    {
        _generator = std::mt19937(_random_device());
        distribution = std::uniform_int_distribution<>(0, CHARACTERS.size() - 1);
    }

    public:
    static random_generator *
    getInstance()
    {
        if (!instance)
            instance = new random_generator;
        return (instance);
    }

    std::string
    generate_string(std::size_t length)
    {
        std::string random_string;

        if (length == 0)
            throw std::invalid_argument("random_generator.generate_string: 0 is an invalid length");

        for (std::size_t i = 0; i < length; ++i)
            random_string += CHARACTERS[distribution(_generator)];

        return (random_string);
    }
};
random_generator *random_generator::instance = 0;
} // namespace test_harness

#endif
