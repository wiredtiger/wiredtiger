#include <vector>
#include <iostream>
#include "session_sim.h"

class Connection {
    std::vector<Session*> session_list;

    public:
        Connection();
        int open_session();
        int query_timestamp();
        int set_timestamp();
};

