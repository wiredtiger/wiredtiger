#ifndef _WT_DBG_EXT_MOCK_FS_H_
#define _WT_DBG_EXT_MOCK_FS_H_

#include "wiredtiger.h"

#define WTC_TRY_BEGIN(msg) try{g_mock_fs.check();

// std::cout << msg << " :begin" << std::endl;

#define WTC_TRY_END(msg) \
    }catch(std::exception& e){ \
        std::cout << msg << " exception: " << e.what() << std::endl; \
        return EINVAL; \
    } \
    return 0;


class MockFS{
public:
    MockFS();
    ~MockFS();

public:
    void init(WT_CONNECTION* conn, WT_CONFIG_ARG* config);
    void check();
    void shutdown();

private:
    WT_EXTENSION_API *m_wtext;
    WT_FILE_SYSTEM m_wtfs;
    bool m_active;
};

extern MockFS g_mock_fs;

#endif