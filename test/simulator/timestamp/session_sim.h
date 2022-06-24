#include <iostream>
#include <vector>
#include "cursor_sim.h"

class Session {
    int txn;
    std::vector<Cursor*> cursor_list;

    public:
        Session ();

};

