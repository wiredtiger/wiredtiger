#include <iostream>
#include <vector>
#include <map>
#include "cursor_sim.h"

class Session {
    int txn;
    std::vector<Cursor*> cursor_list;

    // Could use a map here for cursor 
    // std::map<int, Cursor*> cursor_list;

    public:
        Session ();

};

