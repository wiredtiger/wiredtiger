#include <iostream>
#include <filesystem>
extern "C" {
    #include "wiredtiger.h"
}

int main(int argc, char *argv[]) {
    WT_CONNECTION *conn;
    int ret = 0;
    std::string default_dir = "WT_TEST";
    std::filesystem::create_directory(default_dir);
    ret = wiredtiger_open(default_dir.c_str(), NULL, "create,cache_size=1G", &conn);
    return (ret);
}
