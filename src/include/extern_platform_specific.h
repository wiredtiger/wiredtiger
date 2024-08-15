#ifdef _WIN32
#include "../include/extern_win.h"
#else
#include "../include/extern_posix.h"
#ifdef __linux__
#include "../include/extern_linux.h"
#elif __APPLE__
#include "../include/extern_darwin.h"
#endif
#endif
