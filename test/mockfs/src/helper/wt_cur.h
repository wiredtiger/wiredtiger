#ifndef _WT_DBG_FILTER_WT_CUR_H_
#define _WT_DBG_FILTER_WT_CUR_H_

#include "helper/wt.h"
#include "helper/wt_cfg.h"

class WTCur{
public:
    WTCur(WConn& conn, std::string table, WTConfig& config);
    ~WTCur();

public:
    void set(std::string k, std::string v);
    bool search(std::string k, std::string &v);
    bool update(std::string k, std::string v);
    bool remove(std::string k);
    void dump();

public:
    WT_CURSOR* m_cursor;
    WT_SESSION* m_session;
};

#endif
