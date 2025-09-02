#include <iostream>
#include <dirent.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <signal.h>
#include "ext/mock_fs.h"
#include "ext/mock_file.h"

enum{
    MOCK_FS_EVT_DEACTIVATE = 1
};

/*
 * Forward function declarations for file system API implementation
 */
static int mock_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);

// This is a open class, which means the resource will not auto released
class CStringList{
public:
    CStringList(char** &list, int count = 0)
    :m_count(count), m_list(list)
    {
        m_size = count;
        m_step = 100;
    }

    void append(const char* str){
        while(m_count >= m_size){
            m_size += m_step;
            m_list = (char**)realloc(m_list, sizeof(char*) * m_size);
            if(NULL == m_list){
                throw std::bad_alloc();
            }
        }
        m_list[m_count++] = strdup(str);
    }

    void list_free(){
        for(int i=0; i<m_count; ++i){
            free(m_list[i]);
        }
        free(m_list);
    }

    ~CStringList(){}
public:
    int m_count;

private:
    char** &m_list;
    int m_size;
    int m_step;
};

static int
mock_fs_open(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    const char *name, 
    WT_FS_OPEN_FILE_TYPE file_type, 
    uint32_t flags, 
    WT_FILE_HANDLE **file_handlep)
{
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_open " << name)
    *file_handlep = &MockFile::create(name, file_type, flags)->m_wtfh;
    WTC_TRY_END("mock_fs_open " << name)
}

static int mock_fs_directory_list(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    const char *directory,
    const char *prefix, 
    char ***dirlistp, 
    uint32_t *countp){
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_directory_list")
    DIR* dirp;
    struct dirent *dp;
    char **dirret = NULL;
    dirp = opendir(directory);
    if(NULL == dirp){
        std::cout << "open dir " << directory << " failed: " << strerror(errno) << std::endl;
        return EINVAL;
    }
    CStringList cstr_list(dirret);
    errno = 0;
    while((dp = readdir(dirp)) != NULL) {
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
            continue;
        if (prefix != NULL && strncmp(dp->d_name, prefix, strlen(prefix)) != 0)
            continue;
        cstr_list.append(dp->d_name);
    }
    *dirlistp = dirret;
    *countp = cstr_list.m_count;
    WTC_TRY_END("mock_fs_directory_list")
}

static int mock_fs_directory_list_free(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    char **dirlist, 
    uint32_t count){
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_directory_list_free")
    CStringList cstr_list(dirlist, count);
    cstr_list.list_free();
    WTC_TRY_END("mock_fs_directory_list_free")
}

static int mock_fs_exist(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    const char *name, 
    bool *existp){
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_exist")
    struct stat sb;
    int ret = stat(name, &sb);
    if(0 == ret){
        *existp = true;
    }else{
        // assume the only error is file not exist
        *existp = false;
    }
    WTC_TRY_END("mock_fs_exist")
}

static int mock_fs_remove(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    const char *name, 
    uint32_t flags){
    (void)file_system;
    (void)session;
    (void)flags;
    WTC_TRY_BEGIN("mock_fs_remove")
    // not likely to be error, not for production use
    unlink(name);
    WTC_TRY_END("mock_fs_remove")
}

static int mock_fs_rename(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session,
    const char *from, 
    const char *to,
    uint32_t flags){
    (void)file_system;
    (void)session;
    (void)flags;
    WTC_TRY_BEGIN("mock_fs_rename")
    // not likely to be error, not for production use
    rename(from, to);
    WTC_TRY_END("mock_fs_rename")
}

static int mock_fs_size(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session, 
    const char *name, 
    wt_off_t *sizep){
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_size")
    struct stat sb;
    int ret = stat(name, &sb);
    if(0 == ret){
        *sizep = sb.st_size;
    }else{
        return EINVAL;
    }
    WTC_TRY_END("mock_fs_size")
}

static int
mock_fs_terminate(
    WT_FILE_SYSTEM *file_system, WT_SESSION *session){
    (void)file_system;
    (void)session;
    WTC_TRY_BEGIN("mock_fs_terminate")
    MockFile::clear_files();
    WTC_TRY_END("mock_fs_terminate")
}

MockFS::MockFS(){
    m_wtext = nullptr;
    m_active = true;
    m_wtfs.fs_directory_list = mock_fs_directory_list;
    m_wtfs.fs_directory_list_free = mock_fs_directory_list_free;
    m_wtfs.fs_exist = mock_fs_exist;
    m_wtfs.fs_open_file = mock_fs_open;
    m_wtfs.fs_remove = mock_fs_remove;
    m_wtfs.fs_rename = mock_fs_rename;
    m_wtfs.fs_size = mock_fs_size;
    m_wtfs.terminate = mock_fs_terminate;
}

MockFS::~MockFS(){

}

void MockFS::check(){
    if(!m_active){
        throw std::runtime_error("MockFS id deactived");
    }
}

void MockFS::shutdown(){
    m_active = false;
}

void MockFS::init(WT_CONNECTION *conn, WT_CONFIG_ARG *config){
    (void)config;
    if(nullptr != m_wtext){
        std::cout << "MockFS already initialized" << std::endl;
        return;
    }
    m_wtext = conn->get_extension_api(conn);
    if(0 != conn->set_file_system(conn, &m_wtfs, NULL)){
        throw std::runtime_error("set filesystem to connection failed");
    }
}

static void mock_file_system_signal_handler(int sig, siginfo_t* info, void* context) {  
    if (sig == SIGUSR1) {  
        printf("Received SIGUSR1 with payload: %d\n", info->si_value.sival_int);
        switch(info->si_value.sival_int){
            case MOCK_FS_EVT_DEACTIVATE:
                g_mock_fs.shutdown();
                break;
            default:
                break;
        }
    }  
}

extern "C" __attribute__((visibility("default")))
int mock_file_system_create(WT_CONNECTION *conn, WT_CONFIG_ARG *config){
    WTC_TRY_BEGIN("mock_file_system_create")
    g_mock_fs.init(conn, config);
    // setup signal handler
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = mock_file_system_signal_handler;
    if (sigaction(SIGUSR1, &sa, NULL) == -1) {  
        perror("sigaction register for SIGUSR1 failed");
        exit(EXIT_FAILURE);
    }
    WTC_TRY_END("mock_file_system_create")
}

MockFS g_mock_fs;
