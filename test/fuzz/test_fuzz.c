#include <stdint.h>
#include <stddef.h>
#include <assert.h>

#include "test_util.h"

WT_CONNECTION *conn = NULL;
WT_SESSION *session = NULL;

/*
 * __setup --
 *     Initialise the connection and session the first time LibFuzzer executes the target.
 */
static void
__setup()
{
    if (conn != NULL) {
        assert(session != NULL);
        return;
    }
    testutil_check(wiredtiger_open("WT_TEST", NULL, "create,cache_size=5MB", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
}

typedef struct {
    const uint8_t *key;
    size_t key_size;
    const uint8_t *config;
    size_t config_size;
} FUZZ_INPUT;

/*
 * __parse_input --
 *     This fuzz target accepts two inputs: a key and a configuration string. These two components
 *     are separated by a 4 byte 0xff sequence. This function is designed to check that the input
 *     conforms to this format and if so, returns the index to the beginning of the sequence.
 */
static FUZZ_INPUT
__parse_input(const uint8_t *data, size_t size)
{
    FUZZ_INPUT input;
    const uint8_t separator[] = {0xde, 0xad, 0xbe, 0xef};
    u_int i;

    input.key_size = -1;

    /* Find the first and only separator. */
    const uint8_t *pos = memmem(data, size, separator, sizeof(separator));
    if (pos == NULL)
        return input;

    /* Ensure that there aren't more separators. */
    if (memmem(pos + sizeof(separator), data + size - (pos + sizeof(separator)), separator,
          sizeof(separator)) != NULL)
        return input;

    input.key = data;
    input.key_size = pos - data;
    pos += sizeof(separator);
    input.config = pos;
    input.config_size = size - (pos - data);

    return (input);
}

/*
 * LLVMFuzzerTestOneInput --
 *    A target for LLVM LibFuzzer that tests configuration parsing.
 */
int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    FUZZ_INPUT input;
    WT_CONFIG_ITEM cval;
    u_int i;
    int config_size;
    char *config, *key;

    config = key = NULL;

    __setup();
    input = __parse_input(data, size);
    if (input.key_size == -1)
        return (0);

    /* Convert to C strings. */
    key = malloc(input.key_size + 1);
    memcpy(key, input.key, input.key_size);
    key[input.key_size] = '\0';

    config = malloc(input.config_size + 1);
    memcpy(config, input.config, input.config_size);
    config[input.config_size] = '\0';

    (void)__wt_config_getones((WT_SESSION_IMPL *)session, config, key, &cval);
    (void)cval;

    free(config);
    free(key);
    return (0);
}
