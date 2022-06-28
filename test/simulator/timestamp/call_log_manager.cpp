#include "call_log_manager.h"

CallLogManager::CallLogManager(){
    std::ifstream i("/home/ubuntu/wiredtiger/test/simulator/timestamp/wt_call_log copy.json");
    i >> j;
}

void CallLogManager::dumpCallLog(){
    std::cout << j.dump(4) << std::endl;
}

void CallLogManager::processCallLog(){
    // Loop over each operation in the call log.
    for (const auto& element : j){
        std::cout << element << std::endl;
        processCallLogEntry(element);
    }
}

void CallLogManager::processCallLogEntry(json j){
    std::cout << j << std::endl;


    // If the call log entry is wiredtiger_open -> Create a new connection object.    
    if (j["Operation"]["MethodName"] == "wiredtiger_open"){
        Connection *conn = new Connection();

        // Get the connection objectid from the call log entry.
        std::string s = j["Operation"]["Output"]["objectId"];
        unsigned int x = std::stoul(s, nullptr, 16);

        // Check to see if the object id is the same after changing the type.
        std::cout << std::hex << x << std::endl;

        // Show that open session creates a new session object.
        conn->open_session();

        delete(conn);
    }
}