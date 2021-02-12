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
    std::random_device _rd;
    std::mt19937 _generator;

    random_generator()
    {
        _generator = std::mt19937(_rd());
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
    generate_string(std::size_t len = 20)
    {
        // TODO
        // To improve
        std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

        std::shuffle(str.begin(), str.end(), _generator);

        return str.substr(0, len);
    }
};
random_generator *random_generator::instance = 0;
} // namespace random_generator

#endif
