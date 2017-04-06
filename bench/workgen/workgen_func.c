#include "wiredtiger.h"
#include "test_util.h"

/*
 * These functions call their wiredtiger equivalents.
 */
uint32_t
workgen_random(void volatile * rnd_state)
{
	return (__wt_random((WT_RAND_STATE *)rnd_state));
}

void
workgen_random_init(void volatile * rnd_state)
{
	return (__wt_random_init((WT_RAND_STATE *)rnd_state));
}

/*TODO: better to have alloc/free functions with an opaque type. */
size_t
workgen_random_init_size()
{
	return (sizeof(WT_RAND_STATE));
}
