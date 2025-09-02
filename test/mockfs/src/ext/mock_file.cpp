#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <sys/stat.h>
#include "ext/mock_file.h"

#define BT(a, b) (0 != ((a) & (b)))

/*
 * Forward function declarations for file handle API implementation
 */
static int mock_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session){
    (void)session;
    WTC_TRY_BEGIN("mock_file_close")
    MockFile* mf = MockFile::instance(file_handle);
    if(nullptr != mf){
        MockFile::remove(file_handle);
        return 0;
    }else{
        return EINVAL;
    }
    WTC_TRY_END("mock_file_close")
}

static int mock_file_lock(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, 
    bool lock){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_lock")
    MockFile::instance(file_handle)->f_lock(lock);
    WTC_TRY_END("mock_file_lock")
}

static int mock_file_read(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, 
    wt_off_t offset, 
    size_t len, 
    void *buf_s){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_read")
    MockFile* mf = MockFile::instance(file_handle);
    for(uint8_t* buf = (uint8_t*)buf_s; 
        len > 0; ){
        ssize_t req_len = (int)len;
        if(req_len > 1<<30){
            // from WT_GIGABYTE
            req_len = 1<<30;
        }
        ssize_t nr = mf->f_read(buf, req_len, offset);
        if(nr <= 0){
            return EINVAL;
        }
        buf += nr;
        len -= nr;
        offset += nr;
        if(nr < req_len){
            // this is end
            throw std::runtime_error("read less than request, shouldn't happen");
        }
    }
    WTC_TRY_END("mock_file_read")
}

static int mock_file_size(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_size")
    *sizep = MockFile::instance(file_handle)->f_size();
    WTC_TRY_END("mock_file_size")
}

static int mock_file_sync(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_sync")
    MockFile::instance(file_handle)->f_sync();
    WTC_TRY_END("mock_file_sync")
}

static int mock_file_truncate(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, 
    wt_off_t len){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_truncate")
    MockFile::instance(file_handle)->f_truncate(len);
    WTC_TRY_END("mock_file_truncate")
}

static int mock_file_write(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, 
    wt_off_t offset, 
    size_t len, 
    const void *buf_s){
    (void)wt_session;
    WTC_TRY_BEGIN("mock_file_write")
    MockFile* mf = MockFile::instance(file_handle);
    for(uint8_t* buf = (uint8_t*)buf_s; 
        len > 0; ){
        ssize_t req_len = (int)len;
        if(req_len > 1<<30){
            // from WT_GIGABYTE
            req_len = 1<<30;
        }
        ssize_t nr = mf->f_write(buf, req_len, offset);
        if(nr <= 0){
            return EINVAL;
        }
        buf += nr;
        len -= nr;
        offset += nr;
        if(nr < req_len){
            // this is end
            throw std::runtime_error("read less than request, shouldn't happen");
        }
    }
    WTC_TRY_END("mock_file_write")
}

// wr block first

WRBlock::WRBlock(int fd, uint8_t* buf, size_t size, wt_off_t offset)
    :m_executed(false), fd(fd), offset(offset), size(size){
    m_ts = std::chrono::steady_clock::now();
    data = new uint8_t[size];
    memcpy(data, buf, size);
}

WRBlock::~WRBlock(){
    delete[] data;
}

bool WRBlock::check_delay(int ms){
    if(ms <= 0)
        return true;
    auto now = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_ts).count();
    return dur >= ms;
}

int WRBlock::execute(){
    if(!m_executed){
        m_executed = true;
        ssize_t nw = pwrite(fd, data, size, offset);
        if(nw != (ssize_t)size){
            throw std::runtime_error("WRBlock write failed");
        }
    }
    return 0;
}

MockFile::MockFile(std::string name, WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, int delay_wr_ms)
    :m_name(name), m_file_type(file_type), m_delay_wr_ms(delay_wr_ms){
    m_wtfh.name = strdup(m_name.c_str());
    m_wtfh.close = mock_file_close;
    m_wtfh.fh_advise = NULL;
    m_wtfh.fh_extend = NULL;
    m_wtfh.fh_extend_nolock = NULL;
    m_wtfh.fh_lock = mock_file_lock;
    // Skip MMAP support
    m_wtfh.fh_map = NULL;
    m_wtfh.fh_map_discard = NULL;
    m_wtfh.fh_map_preload = NULL;
    m_wtfh.fh_unmap = NULL;

    m_wtfh.fh_read = mock_file_read;
    m_wtfh.fh_size = mock_file_size;
    m_wtfh.fh_sync = mock_file_sync;
    m_wtfh.fh_sync_nowait = NULL;
    m_wtfh.fh_truncate = mock_file_truncate;
    m_wtfh.fh_write = mock_file_write;
    m_fd = 0;
    f_open(
        BT(flags, WT_FS_OPEN_READONLY),
        BT(flags, WT_FS_OPEN_CREATE),
        BT(flags, WT_FS_OPEN_EXCLUSIVE)
    );
    // skip durable first : if(BT(flags, WT_FS_OPEN_DURABLE))
    // skip fadvise, not for performance test
}

MockFile::~MockFile(){
    if(0 != m_fd){
        sync();
        close(m_fd);
        m_fd = 0;
    }
    free(m_wtfh.name);
}

void MockFile::f_open(bool readonly, bool create, bool exclusive){
    int flag = 0;
    mode_t mode = 0;
    if(WT_FS_OPEN_FILE_TYPE_DIRECTORY == m_file_type){
        flag |= O_RDONLY;
        mode = 0444;
    }else{
        if(readonly)
            flag |= O_RDONLY;
        else
            flag |= O_RDWR;
        if(create){
            mode = 0666;
            flag |= O_CREAT;
            if(exclusive)
                flag |= O_EXCL;
        }else{
            mode = 0;
        }
        if(WT_FS_OPEN_FILE_TYPE_LOG == m_file_type){
            // as we control the write manually, not that matter
            flag |= O_SYNC;
        }
    }
    m_fd = open(m_name.c_str(), flag, mode);
    if(m_fd < 0)
        throw std::runtime_error("open file failed " + m_name);
}

void MockFile::f_lock(bool lock){
    struct flock fl;
    memset(&fl, 0, sizeof(fl));
    fl.l_type = lock ? F_WRLCK : F_UNLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 1; // whole file
    int ret = fcntl(m_fd, F_SETLK, &fl);
    if(ret == -1){
        perror("fcntl failed");
        throw std::runtime_error("fcntl failed " + m_name + (lock ? "lock" : "unlock"));
    }
}

void MockFile::f_truncate(off_t size){
    sync();
    ftruncate(m_fd, size);
    // as we didn't use mmap, no need to update buffer
}

void MockFile::f_sync(){
    sync();
    int ret = fsync(m_fd);
    if(ret == -1){
        perror("fsync failed");
        throw std::runtime_error("fsync failed " + m_name);
    }
}

ssize_t MockFile::f_read(uint8_t* buf, size_t size, wt_off_t offset){
    sync();
    return pread(m_fd, buf, size, offset);
}

ssize_t MockFile::f_write(uint8_t* buf, size_t size, wt_off_t offset){
    std::unique_ptr<WRBlock> wrb(new WRBlock(m_fd, buf, size, offset));
    {
        std::lock_guard<std::mutex> lg(m_wrb_mutex);
        m_write_blocks.push_back(std::move(wrb));
    }
    sync(m_delay_wr_ms);
    return size;
    // return pwrite(m_fd, buf, size, offset);
}

void MockFile::sync(int ms){
    std::lock_guard<std::mutex> lg(m_wrb_mutex);
    while(!m_write_blocks.empty()){
        auto& wrb = m_write_blocks.front();
        if(wrb->check_delay(ms)){
            wrb->execute();
            m_write_blocks.pop_front();
        }else{
            break;
        }
    }
}

off_t MockFile::f_size(){
    struct stat sb;
    int ret = fstat(m_fd, &sb);
    if (ret == 0) {
        return sb.st_size;
    }
    throw std::runtime_error("fstat failed " + m_name);
}

MockFile* MockFile::instance(WT_FILE_HANDLE* fh){
    auto it = m_files.find(fh);
    if(it != m_files.end()){
        return it->second.get();
    }else{
        return nullptr;
    }
}

MockFile* MockFile::create(std::string name, WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags){
    std::unique_ptr<MockFile> mf(new MockFile(name, file_type, flags, 100));
    auto res = m_files.emplace(&mf->m_wtfh, std::move(mf));
    if(res.second){
        return res.first->second.get();
    }else{
        return nullptr;
    }
}

void MockFile::remove(WT_FILE_HANDLE* fh){
    m_files.erase(fh);
}

void MockFile::clear_files(){
    m_files.clear();
}

std::unordered_map<WT_FILE_HANDLE*, std::unique_ptr<MockFile>> MockFile::m_files;
