#include "helper/wt_cur.h"
#include <iostream>

WTCur::WTCur(WConn& conn, std::string table, WTConfig& config){
    m_session = conn.get_session();
    do{
        int ret = m_session->open_cursor(
            m_session,
            table.c_str(),
            NULL,
            NULL,
            &m_cursor
        );
        if(ret == 0){
            break;
        }else{
            // table not exist, create it
            std::string cur_conf = config.str();
            wt_assert(m_session->create(
                    m_session,
                    table.c_str(),
                    cur_conf.c_str()
                ),
                "create table for %s in %s", table.c_str(), cur_conf.c_str()
            );
        }
    }while(true);
}

WTCur::~WTCur(){
    m_cursor->close(m_cursor);
}

void WTCur::set(std::string k, std::string v){
    m_cursor->set_key(m_cursor, k.c_str());
    m_cursor->set_value(m_cursor, v.c_str());
    wt_assert(m_cursor->insert(m_cursor),
        "insert k:%s, v:%s", k.c_str(), v.c_str()
    );
}

bool WTCur::update(std::string k, std::string v){
    m_cursor->set_key(m_cursor, k.c_str());
    int ret = m_cursor->search(m_cursor);
    const char* v_s;
    if(ret){
        if(WT_NOTFOUND == ret){
            return false;
        }
        wt_assert(ret, "update for %s failed", k.c_str());
    }
    m_cursor->set_value(m_cursor, v.c_str());
    m_cursor->update(m_cursor);
    return true;
}

bool WTCur::remove(std::string k){
    m_cursor->set_key(m_cursor, k.c_str());
    int ret = m_cursor->remove(m_cursor);
    if(ret){
        if(WT_NOTFOUND == ret){
            return false;
        }
        wt_assert(ret, "remove for %s failed", k.c_str());
    }
    return true;
}


bool WTCur::search(std::string k, std::string &v){
    m_cursor->set_key(m_cursor, k.c_str());
    int ret = m_cursor->search(m_cursor);
    const char* v_s;
    if(ret){
        if(WT_NOTFOUND == ret){
            return false;
        }
        wt_assert(ret, "search for %s failed", k.c_str());
    }
    m_cursor->get_value(m_cursor, &v_s);
    v = v_s;
    return true;
}

void WTCur::dump(){
    std::ostringstream oss;
    wt_assert(m_cursor->reset(m_cursor),
        "cusor reset"
    );
    do{
        int ret = m_cursor->next(m_cursor);
        if(WT_NOTFOUND == ret){
            break;
        }else if(ret){
            wt_assert(ret, "cursor fetch failed");
        }
        const char *k;
        const char *v;
        m_cursor->get_key(m_cursor, &k);
        m_cursor->get_value(m_cursor, &v);
        oss << k << ":" << v << std::endl;
    }while(1);
    std::cout << "table fetch:" << std::endl << oss.str() << std::endl;
}
