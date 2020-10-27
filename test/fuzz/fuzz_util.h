#include "test_util.h"

typedef struct {
    WT_CONNECTION *conn;
    WT_SESSION *session;
} FUZZ_GLOBAL_STATE;

typedef struct {
    const uint8_t **slices;
    size_t *sizes;
    size_t num_slices;
} FUZZ_SLICED_INPUT;

extern FUZZ_GLOBAL_STATE fuzz_state;

void __fuzz_setup();
bool __fuzz_sliced_input_init(
  const uint8_t *data, size_t size, FUZZ_SLICED_INPUT *input, size_t required_slices);
void __fuzz_sliced_input_free(FUZZ_SLICED_INPUT *input);
char *__fuzz_slice_to_cstring(const uint8_t *data, size_t size);
