#include "connection_sim.h"

Connection::Connection (){
    std::cout << "Connection constructor: Creating connection..." << std::endl;
}

int Connection::open_session(){
    std::cout << "Connection: opening_session" << std::endl;
    Session* s = new Session();

    session_list.push_back(s);

    return (0);
}
