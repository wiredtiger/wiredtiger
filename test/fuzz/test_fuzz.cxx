#include <cstdint>
#include <cstddef>

#include <string>
#include <iostream>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    std::string s((const char *)data, size);
    std::cout << "Printing some fuzz data of size " << size << ":\n";
    std::cout << s << std::endl;
    return 0;
}
