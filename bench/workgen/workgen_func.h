struct workgen_random_state;

extern uint32_t
workgen_atomic_add32(uint32_t *vp, uint32_t v);
extern uint64_t
workgen_atomic_add64(uint64_t *vp, uint64_t v);
extern void
workgen_epoch(struct timespec *tsp);
extern uint32_t
workgen_random(struct workgen_random_state volatile *rnd_state);
extern int
workgen_random_alloc(WT_SESSION *session,
  struct workgen_random_state **rnd_state);
extern void
workgen_random_free(struct workgen_random_state *rnd_state);
extern void
workgen_u64_to_string_zf(uint64_t n, char *buf, size_t len);
