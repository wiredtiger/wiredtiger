#include "helper/wt.h"

thread_local WT_SESSION* WConn::m_session = nullptr;

WConn::WConn(std::string wt_home, WTConfig& cfg){
    m_conn = NULL;
    std::string cfg_str = cfg.str();
    wt_assert(
        wiredtiger_open(wt_home.c_str(), NULL, cfg_str.c_str(), &m_conn),
        "open wired tiger at %s", wt_home.c_str()
    );
}

WT_SESSION* WConn::get_session(){
    if(nullptr == m_session){
        wt_assert(
            m_conn->open_session(m_conn, NULL, NULL, &m_session),
            "open session"
        );
        m_sessions.push_back(m_session);
    }
    return m_session;
}

WConn::~WConn(){
    if(m_conn){
        for(auto session : m_sessions){
            session->close(session, NULL);
        }
    }
}

void WConn::u_test(){
}

void wt_assert(int v, const char *fmt, ...){
    if(v == 0){
        return;
    }
    va_list ap;

    /* Flush output to be sure it doesn't mix with fatal errors. */
    (void)fflush(stdout);
    (void)fflush(stderr);

    fprintf(stderr, "FAILED");
    if (fmt != NULL) {
        fprintf(stderr, ": ");
        va_start(ap, fmt);
        vfprintf(stderr, fmt, ap);
        va_end(ap);
    }
    fprintf(stderr, ": %s", wiredtiger_strerror(v));
    fprintf(stderr, "\n");
    (void)fflush(stderr);

    /* Allow test programs to cleanup on fatal error. */

    /* Drop core. */
    fprintf(stderr, "process aborting\n");
    exit(1);
}
