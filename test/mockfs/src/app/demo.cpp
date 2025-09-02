#include <sys/stat.h>  
#include <sys/types.h> 
#include <iostream> 
#include <thread>
#include <random> 
#include <signal.h>
#include "helper/wt.h"
#include "helper/wt_cur.h"

std::string generateRandomString(size_t length) {  
    // Define the character set for the random string (alphanumerics in this case)  
    const std::string characters = "abcdefghijklmnopqrstuvwxyz";  
      
    // Seed for random number generator  
    std::random_device rd;                    // Obtain a random number from hardware  
    std::mt19937 generator(rd());             // Seed the generator  
    std::uniform_int_distribution<size_t> distribution(0, characters.size() - 1); // Range is the size of the character set  
  
    std::string randomString;                 // The resulting random string  
  
    for (size_t i = 0; i < length; ++i) {  
        randomString += characters[distribution(generator)];  
    }  
      
    return randomString;  
}

void update_test(WConn& conn){
    int done = 0;
        WTCur cur(
            conn, 
            "table:access", 
            WTConfig().append("key_format", "S").append("value_format","S")
        );
    for(int rnd = 0; rnd < 3000000; rnd ++){
        std::cout << rnd << " Round start" << std::endl;
        std::vector<std::string> rand_keys;
        for(int i = 0; i < 100; i++){
            rand_keys.push_back(generateRandomString(10));
        }
        for(int i = 0; i < 100000; i++){
            std::string k = generateRandomString(10);
            cur.set(k, std::to_string(i));
            cur.update(k, "185");
            std::string v;
            if(cur.search(k, v)){
                // std::cout << "fetch a with v:" << v << std::endl;
            }else{
                // std::cout << "record of a not found" << std::endl;
            }
            done ++;
            if(done % 10000 == 0){
                
            }
            // cur.remove(k);
        }
        {
            // sig test
            pid_t pid = getpid();
            union sigval value;
            value.sival_int = rnd;
            if (sigqueue(pid, SIGUSR1, value) == -1) {
                perror("sigqueue failed");
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
}

WTConfig& append_extension_config(WTConfig& wt_cfg){
    wt_cfg.append("extensions", 
        WTConfig().append("libwiredtiger_mockfs.so",
            WTConfig().append("entry", "mock_file_system_create")
                .append("early_load", "true")
                .append("config", 
                    WTConfig().append("config_string", "demo")
                        .append("value", "30")
                )
        )
    );
    return wt_cfg;
}

void create_db_home_if_not_exist(const char* home){
    struct stat info;
    if(stat(home, &info) != 0){
        // not exist
        if(mkdir(home, 0755) != 0){
            throw std::runtime_error("create db home failed");
        }
    }else if(!(info.st_mode & S_IFDIR)){
        throw std::runtime_error("db home is not a directory");
    }
}

int main(int argc, char* argv[]){
    const char* wt_home = std::getenv("DBG_WTHOME");
    create_db_home_if_not_exist(wt_home);
    std::cout << "Config string: " << append_extension_config(
        WTConfig().append("create")
            .append("cache_size", "100MB")
    ).str() << std::endl;
    WConn conn(
        wt_home, 
        append_extension_config(
            WTConfig().append("create")
                .append("cache_size", "100MB")
        )
    );
    update_test(conn);
    return 0;
}