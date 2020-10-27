#include <assert.h>
#include <stdint.h>
#include <stddef.h>

#include "fuzz_util.h"

FUZZ_GLOBAL_STATE fuzz_state = {.conn = NULL, .session = NULL};

static void
__fuzz_generate_home_name(char *buf)
{
    pthread_t thandle;

    thandle = pthread_self();
    sprintf(buf, "WT_TEST_%p", (void *)thandle);
}

/*
 * __fuzz_setup --
 *     Initialize the connection and session the first time LibFuzzer executes the target.
 */
void
__fuzz_setup()
{
    char home[100];

    if (fuzz_state.conn != NULL) {
        assert(fuzz_state.session != NULL);
        return;
    }

    WT_CLEAR(home);
    __fuzz_generate_home_name(home);
    testutil_make_work_dir(home);
    testutil_check(wiredtiger_open(home, NULL, "create,cache_size=5MB", &fuzz_state.conn));
    testutil_check(fuzz_state.conn->open_session(fuzz_state.conn, NULL, NULL, &fuzz_state.session));
}

bool
__fuzz_sliced_input_init(
  const uint8_t *data, size_t size, FUZZ_SLICED_INPUT *input, size_t required_slices)
{
    static const uint8_t separator[] = {0xde, 0xad, 0xbe, 0xef};
    const uint8_t *begin, **slices, *end, *pos;
    size_t *sizes;
    u_int i;

    pos = NULL;
    i = 0;
    begin = data;
    end = data + size;

    slices = malloc(sizeof(uint8_t *) * required_slices);
    sizes = malloc(sizeof(size_t) * required_slices);
    if (!slices || !sizes)
        goto err;

    while ((pos = memmem(begin, end - begin, separator, sizeof(separator))) != NULL) {
        if (i >= required_slices)
            goto err;
        slices[i] = begin;
        sizes[i] = pos - begin;
        begin = pos + sizeof(separator);
        ++i;
    }
    if (begin < end) {
        if (i >= required_slices)
            goto err;
        slices[i] = begin;
        sizes[i] = end - begin;
        ++i;
    }
    if (i != required_slices)
        goto err;
    input->slices = slices;
    input->sizes = sizes;
    input->num_slices = required_slices;
    return (true);

err:
    free(slices);
    free(sizes);
    return (false);
}

void
__fuzz_sliced_input_free(FUZZ_SLICED_INPUT *input)
{
    free(input->slices);
    free(input->sizes);
}

char *
__fuzz_slice_to_cstring(const uint8_t *data, size_t size)
{
    char *str;

    str = malloc(size + 1);
    if (str == NULL)
        return NULL;
    memcpy(str, data, size);
    str[size] = '\0';

    return str;
}
