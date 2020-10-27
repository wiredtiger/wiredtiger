#include "fuzz_util.h"

#include <assert.h>

/*
 * LLVMFuzzerTestOneInput --
 *    A target for LLVM LibFuzzer that tests configuration parsing.
 */
int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    FUZZ_SLICED_INPUT input;
    WT_CONFIG_ITEM cval;
    char *config, *key;

    WT_CLEAR(input);
    config = key = NULL;

    __fuzz_setup();
    if (!__fuzz_sliced_input_init(data, size, &input, 2))
        return (0);

    assert(input.num_slices == 2);
    key = __fuzz_slice_to_cstring(input.slices[0], input.sizes[0]);
    if (key == NULL)
        testutil_die(ENOMEM, "Failed to allocate key");
    config = __fuzz_slice_to_cstring(input.slices[1], input.sizes[1]);
    if (config == NULL)
        testutil_die(ENOMEM, "Failed to allocate config");

    (void)__wt_config_getones((WT_SESSION_IMPL *)fuzz_state.session, config, key, &cval);
    (void)cval;

    __fuzz_sliced_input_free(&input);
    free(config);
    free(key);
    return (0);
}
