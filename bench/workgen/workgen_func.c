#include "wiredtiger.h"
#include "test_util.h"

/* This is an opaque type handle. */
typedef struct workgen_random_state {} workgen_random_state;

/*
 * These functions call their wiredtiger equivalents.
 */
void
workgen_epoch(struct timespec *tsp)
{
    __wt_epoch(NULL, tsp);
}

uint32_t
workgen_random(workgen_random_state volatile * rnd_state)
{
	return (__wt_random((WT_RAND_STATE *)rnd_state));
}

int
workgen_random_alloc(WT_SESSION *session, workgen_random_state **rnd_state)
{
	WT_RAND_STATE *state;
	state = malloc(sizeof(WT_RAND_STATE));
	if (state == NULL) {
		*rnd_state = NULL;
		return (ENOMEM);
	}
	__wt_random_init_seed((WT_SESSION_IMPL *)session, state);
	*rnd_state = (workgen_random_state *)state;
	return (0);
}

void
workgen_random_free(workgen_random_state *rnd_state)
{
	free(rnd_state);
}

extern void
workgen_u64_to_string_zf(uint64_t n, char *buf, size_t len)
{
	u64_to_string_zf(n, buf, len);
}
