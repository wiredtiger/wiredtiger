#ifndef _WT_DBG_FILTER_WT_CFG_H_
#define _WT_DBG_FILTER_WT_CFG_H_

#include <sstream>

class WTConfig{
public:
    WTConfig();
    ~WTConfig();

    WTConfig& append(std::string k);
    WTConfig& append(std::string k, std::string v);
    WTConfig& append(std::string k, WTConfig& v);

    std::string str();

public:
    WTConfig& readonly();
    WTConfig& exclusive(bool exist_ok);
    WTConfig& session_max(int v=100);

private:
    std::ostringstream m_oss;
    bool m_is_first;
};


#endif