#include "helper/wt_cfg.h"

WTConfig::WTConfig(){
    m_is_first = true;
}

WTConfig::~WTConfig(){

}

WTConfig& WTConfig::append(std::string k){
    if(m_is_first){
        m_is_first = false;
    }else{
        m_oss << ",";
    }
    m_oss << k;
    return *this;
}

WTConfig& WTConfig::append(std::string k, std::string v){
    append(k);
    m_oss << "=" << v;
    return *this;
}

WTConfig& WTConfig::append(std::string k, WTConfig& v){
    append(k);
    m_oss << "=(" << v.str() << ")";
    return *this;
}

std::string WTConfig::str(){
    return m_oss.str();
}

WTConfig& WTConfig::readonly(){
    return append("readonly");
}

WTConfig& WTConfig::exclusive(bool exist_ok){
    if(exist_ok){
        return *this;
    }else{
        return append("exclusive");
    }
}

WTConfig& WTConfig::session_max(int v){
    return append("session_max", ""+v);
}

