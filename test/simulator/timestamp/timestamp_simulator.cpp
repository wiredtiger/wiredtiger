#include <iostream>
#include "json.hpp"

using json = nlohmann::json;

int
main()
{
    json j = { {"pi", 3.141}, {"happy", true}};
    std::cout << j.dump(4) << std::endl;
    return 0;
}
