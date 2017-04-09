struct workgen_random_state;

extern void
workgen_epoch(struct timespec *tsp);
extern uint32_t
workgen_random(workgen_random_state volatile *rnd_state);
extern int
workgen_random_alloc(WT_SESSION *session, workgen_random_state **rnd_state);
extern void
workgen_random_free(workgen_random_state *rnd_state);
extern void
workgen_u64_to_string_zf(uint64_t n, char *buf, size_t len);
