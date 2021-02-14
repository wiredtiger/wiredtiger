/* Include guard. */
#ifndef RANDOM_GENERATOR_H
#define RANDOM_GENERATOR_H

#include <cstddef>

#include <random>
#include <string>
#include <algorithm>

namespace random_generator {

class random_generator {

    static random_generator *instance;
    std::random_device _random_device;
    std::mt19937 _generator;

    random_generator()
    {
        _generator = std::mt19937(_random_device());
    }

    public:
    static random_generator *
    getInstance()
    {
        if (!instance)
            instance = new random_generator;
        return instance;
    }

    std::string
    generate_string(std::size_t length = 20)
    {
        const std::string CHARACTERS =
          "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

        std::string random_string;

        for (std::size_t i = 0; i < length; ++i) {
            random_string += CHARACTERS[distribution(_generator)];
        }

        return random_string;
    }
};
random_generator *random_generator::instance = 0;
} // namespace random_generator

#endif
