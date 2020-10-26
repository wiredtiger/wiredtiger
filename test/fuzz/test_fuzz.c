#include <stdint.h>
#include <stddef.h>
#include <assert.h>

#include "test_util.h"

WT_CONNECTION *conn = NULL;
WT_SESSION *session = NULL;

void
setup()
{
    if (conn != NULL) {
        assert(session != NULL);
        return;
    }
    testutil_check(wiredtiger_open("WT_TEST", NULL, "create,cache_size=5MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
}

// Accepts input like: "key|config"
int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    WT_CONFIG_ITEM cval;
    u_int i;
    int config_size;
    char *config, *key;

    config = key = NULL;

    setup();

    for (i = 0; i < size; ++i)
        if (data[i] == '|')
            break;

    // Put together the key.
    key = malloc(i + 1);
    memcpy(key, &data[0], i);
    key[i] = '\0';

    // Skip over the |.
    ++i;

    if (i >= size)
        goto done;

    // The rest of the input is the configuration string.
    config_size = size - i;
    config = malloc(config_size + 1);
    memcpy(config, &data[i], config_size);
    config[config_size] = '\0';

    (void)__wt_config_getones((WT_SESSION_IMPL *)session, config, key, &cval);
    (void)cval;

done:
    free(config);
    free(key);
    return (0);
}
