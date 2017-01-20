#include "workgen.h"
#include <iostream>

namespace workgen {

int execute(Workload &workload) {
    std::cout << "Executing the workload" << std::endl;
    workload.describe(std::cout);

	return (0);
}

};
