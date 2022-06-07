/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "random_generator.h"

#include <algorithm>

extern "C" {
#include "test_util.h"
}

namespace test_harness {
RandomGenerator &
RandomGenerator::GetInstance()
{
    thread_local RandomGenerator _instance;
    return (_instance);
}

std::string
RandomGenerator::GenerateRandomString(std::size_t length, CharactersType type)
{
    const std::string characters = GetCharacters(type);
    std::string str;

    while (str.size() < length)
        str += characters;

    std::shuffle(str.begin(), str.end(), _generator);
    return (str.substr(0, length));
}

std::string
RandomGenerator::GeneratePseudoRandomString(std::size_t length, CharactersType type)
{
    std::string randomString;
    std::uniform_int_distribution<> &distribution = GetDistribution(type);
    std::size_t index = distribution(_generator);
    const std::string &characters = GetCharacters(type);

    for (std::size_t i = 0; i < length; ++i) {
        randomString += characters[index];
        if (index == characters.size() - 1)
            index = 0;
        else
            ++index;
    }
    return (randomString);
}

RandomGenerator::RandomGenerator()
{
    _generator = std::mt19937(std::random_device{}());
    _alphaNumDistribution = std::uniform_int_distribution<>(0, _pseudoAlphaNum.size() - 1);
    _alphaDistribution = std::uniform_int_distribution<>(0, _alphabet.size() - 1);
}

std::uniform_int_distribution<> &
RandomGenerator::GetDistribution(CharactersType type)
{
    switch (type) {
    case CharactersType::kAlphabet:
        return (_alphaDistribution);
    case CharactersType::kPseudoAlphaNum:
        return (_alphaNumDistribution);
    default:
        testutil_die(static_cast<int>(type), "Unexpected CharactersType");
    }
}

const std::string &
RandomGenerator::GetCharacters(CharactersType type)
{
    switch (type) {
    case CharactersType::kAlphabet:
        return (_alphabet);
    case CharactersType::kPseudoAlphaNum:
        return (_pseudoAlphaNum);
    default:
        testutil_die(static_cast<int>(type), "Unexpected CharactersType");
    }
}

} // namespace test_harness
