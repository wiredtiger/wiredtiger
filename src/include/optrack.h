/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */


#define WT_OPTRACK_MAXRECS 16384
#define WT_OPTRACK_BUFSIZE WT_OPTRACK_MAXRECS * sizeof(WT_TRACK_RECORD)

/*
 * WT_TRACK_RECORD --
 *     A structure for logging potentially long operations.
 */
struct __wt_track_record {
	uint64_t timestamp;
	uint64_t op_id;
	uint16_t op_type;
};

#define WT_FUNC_ADDR(s) &__func__

#define WT_TRACK_OP(s, optype)						\
	static volatile bool id_recorded = 0;				\
	WT_TRACK_RECORD *tr = &(s->optrack_buf[s->optrackbuf_ptr++]);	\
	tr->timestamp = __wt_rdtsc();					\
	tr->op_type = optype;						\
	tr->op_id = (uint64_t)WT_FUNC_ADDR(s);				\
	if (optype == 0 && !id_recorded)				\
		__wt_optrack_record_funcid(s, tr->op_id,		\
					   (void*)&__func__,		\
					   sizeof(__func__),		\
					   &id_recorded);		\
	if (s->optrackbuf_ptr == WT_OPTRACK_MAXRECS) {			\
		if (s->optrack_fh != NULL) {				\
			ret = s->optrack_fh->handle->fh_write(		\
				s->optrack_fh->handle, (WT_SESSION *)s,	\
				s->optrack_offset, WT_OPTRACK_BUFSIZE,  \
				s->optrack_buf);			\
			if (ret == 0)                                   \
				s->optrack_offset += WT_OPTRACK_BUFSIZE;\
		}                                                       \
		s->optrackbuf_ptr = 0;					\
	}
