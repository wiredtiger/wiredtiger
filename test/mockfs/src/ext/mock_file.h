#ifndef _WT_DBG_EXT_MOCK_FILE_H_
#define _WT_DBG_EXT_MOCK_FILE_H_

#include <chrono>
// The reason for unordered_map is not too many files
#include <unordered_map>
#include <memory>
#include <deque>
#include <string>
#include <mutex>
#include "helper/wt.h"
#include "ext/mock_fs.h"

class WRBlock{
public:
    WRBlock(int fd, uint8_t* buf, size_t size, wt_off_t offset);
    ~WRBlock();

public:
    int execute();
    // if no parameter is passed, always execute
    bool check_delay(int ms = 0);

public:
    std::chrono::steady_clock::time_point m_ts;
    bool m_executed;
    // directly used 
    int fd;
    wt_off_t offset;
    size_t size;
    uint8_t *data;
};

class MockFile{
public:
    MockFile(std::string name, WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, int delay_wr_ms = 0);
    ~MockFile();

public:
    void f_open(bool readonly, bool create, bool exclusive);
    void f_lock(bool lock);
    ssize_t f_read(uint8_t* buf, size_t size, wt_off_t offset);
    ssize_t f_write(uint8_t* buf, size_t size, wt_off_t offset);
    void f_sync();
    off_t f_size();
    void f_truncate(off_t size);

public:
    static MockFile* create(std::string name, WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags);
    static MockFile* instance(WT_FILE_HANDLE* fh);
    static void remove(WT_FILE_HANDLE* fh);
    static void clear_files();

private:
    static std::unordered_map<WT_FILE_HANDLE*, std::unique_ptr<MockFile>> m_files;

public:
    WT_FILE_HANDLE m_wtfh;

private:
    std::string m_name;
    WT_FS_OPEN_FILE_TYPE m_file_type;
    int m_fd;

private:
    int m_delay_wr_ms;
    std::deque<std::unique_ptr<WRBlock>> m_write_blocks;
    std::mutex m_wrb_mutex;
    void sync(int ms = 0);
};

#endif