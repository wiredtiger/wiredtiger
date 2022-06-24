#include <vector>
#include <iostream>
#include "session_sim.h"

class Connection {
    std::vector<Session*> session_list;
    Session *active_session;

    public:
        Connection();
        int open_session();
        int query_timestamp();
        int set_timestamp();
};

