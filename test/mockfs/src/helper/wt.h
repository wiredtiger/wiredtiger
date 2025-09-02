#ifndef _WT_DBG_FILTER_WT_H_
#define _WT_DBG_FILTER_WT_H_

#include "helper/wt_cfg.h"
#include <vector>

#include "wiredtiger.h"

void wt_assert(int v, const char *fmt, ...);

#define WTC_TRY_BEGIN(msg) try{g_mock_fs.check();

// std::cout << msg << " :begin" << std::endl;

#define WTC_TRY_END(msg) \
    }catch(std::exception& e){ \
        std::cout << msg << " exception: " << e.what() << std::endl; \
        return EINVAL; \
    } \
    return 0;

// std::cout << msg << " :end" << std::endl; 

class WConn{
public:
    WConn(std::string wt_home, WTConfig& config);
    ~WConn();

public:
    WT_SESSION* get_session();

    void u_test();

public:
    std::vector<WT_SESSION *> m_sessions;
    WT_CONNECTION *m_conn;
    static thread_local WT_SESSION *m_session;
};

#endif