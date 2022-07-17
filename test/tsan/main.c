/*
 * Built with:
 * cmake -DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_v3_clang.cmake
 * -DCMAKE_BUILD_TYPE=TSan -DENABLE_PYTHON=1 -DENABLE_LZ4=1 -DENABLE_SNAPPY=1
 * -DENABLE_ZLIB=1 -DENABLE_ZSTD=1 -DHAVE_DIAGNOSTIC=1 -DENABLE_STRICT=1
 * -DCMAKE_EXPORT_COMPILE_COMMANDS=ON . -G Ninja ../.
 */

#include <pthread.h>
#include <stdio.h>

static int global;

static void* thread1(void* x) {
    (void)x;
    global++;
    return NULL;
}

static void* thread2(void* x) {
    (void)x;
    global--;
    return NULL;
}

int main(void) {
    pthread_t t[2];
    pthread_create(&t[0], NULL, thread1, NULL);
    pthread_create(&t[1], NULL, thread2, NULL);
    pthread_join(t[0], NULL);
    pthread_join(t[1], NULL);

    return 0;
}
