#include <assert.h>
#include <stdint.h>
#include <stddef.h>

#include "fuzz_util.h"

FUZZ_GLOBAL_STATE fuzz_state = {.conn = NULL, .session = NULL};

/*
 * fuzzutil_generate_home_name --
 *     Create a unique home directory per thread that LibFuzzer creates.
 */
static void
fuzzutil_generate_home_name(char *buf)
{
    pthread_t thandle;

    /*
     * Good lord. There doesn't seem to a nice POSIX compatible way of doing this. This does the job
     * but the directory names look silly. We can revisit this later if necessary.
     */
    thandle = pthread_self();
    sprintf(buf, "WT_TEST_%p", (void *)thandle);
}

/*
 * fuzzutil_setup --
 *     Initialize the connection and session the first time LibFuzzer executes the target.
 */
void
fuzzutil_setup()
{
    char home[100];

    if (fuzz_state.conn != NULL) {
        assert(fuzz_state.session != NULL);
        return;
    }

    WT_CLEAR(home);
    fuzzutil_generate_home_name(home);
    testutil_make_work_dir(home);
    testutil_check(wiredtiger_open(home, NULL, "create,cache_size=5MB", &fuzz_state.conn));
    testutil_check(fuzz_state.conn->open_session(fuzz_state.conn, NULL, NULL, &fuzz_state.session));
}

/*
 * fuzzutil_sliced_input_init --
 *     Often, our fuzz target requires multiple inputs. For example, for configuration parsing we'd
 *     need a configuration string and a key to search for. We can do this by requiring the fuzzer
 *     to provide data with a number of arbitrary multi-byte separators (in our system, we use
 *     0xdeadbeef). If the fuzzer doesn't supply data in that format, we can return out of the fuzz
 *     target. While our fuzz target will reject lots of input to begin with, the fuzzer will figure
 *     out that inputs with these separators yield better coverage and will craft more sensible
 *     inputs over time. This is what the sliced input component is designed for. It takes the data
 *     input and the number of slices that it should expect and populates a heap allocated array of
 *     data pointers to each separate input and their respective size.
 */
bool
fuzzutil_sliced_input_init(
  const uint8_t *data, size_t size, FUZZ_SLICED_INPUT *input, size_t required_slices)
{
    static const uint8_t separator[] = {0xde, 0xad, 0xbe, 0xef};
    const uint8_t *begin, *end, *pos, **slices;
    size_t *sizes;
    u_int i;

    pos = NULL;
    i = 0;
    begin = data;
    end = data + size;

    /*
     * It might be better to do an initial pass to check that we have the right number of separators
     * before actually storing them. Currently, we're dynamically allocating even in the case of
     * invalid input.
     */
    slices = malloc(sizeof(uint8_t *) * required_slices);
    sizes = malloc(sizeof(size_t) * required_slices);
    if (slices == NULL || sizes == NULL)
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

/*
 * fuzzutil_sliced_input_free --
 *     Free any resources on the sliced input.
 */
void
fuzzutil_sliced_input_free(FUZZ_SLICED_INPUT *input)
{
    free(input->slices);
    free(input->sizes);
}

/*
 * fuzzutil_slice_to_cstring --
 *     A conversion function to help convert from a data, size pair to a cstring.
 */
char *
fuzzutil_slice_to_cstring(const uint8_t *data, size_t size)
{
    char *str;

    str = malloc(size + 1);
    if (str == NULL)
        return NULL;
    memcpy(str, data, size);
    str[size] = '\0';

    return str;
}
