#include <iostream>
#include <map>
#include <fstream>
#include <string>
#include "connection_sim.h"
#include "json.hpp"

using json = nlohmann::json;

class CallLogManager {
    json j;
    std::string callLogPath = "/home/ubuntu/wiredtiger/test/simulator/timestamp/wt_call_log.json";

    public:
        CallLogManager();
        void processCallLog();
        void dumpCallLog();

    private:
        void processCallLogEntry(json j);
};
