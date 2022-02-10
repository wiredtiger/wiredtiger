#include <string>
#include <stdexcept>
#include "error_handler.h"

void
ErrorHandler::throwIfNonZero(int result)
{
    if (result != 0) {
        std::string errorMessage(
          "Error result in ErrorHandler::ThrowIfNonZero is " + std::to_string(result));
        throw std::runtime_error(errorMessage);
    }
}
