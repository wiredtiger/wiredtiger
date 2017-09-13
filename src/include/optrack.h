/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define	WT_OPTRACK_MAXRECS 16384
#define	WT_OPTRACK_BUFSIZE WT_OPTRACK_MAXRECS * sizeof(WT_TRACK_RECORD)

/*
 * WT_TRACK_RECORD --
 *     A structure for logging potentially long operations.
 *
 * We pad the record so that the size of the entire record is
 * three double words, or 24 bytes. If we don't do this, the compiler
 * will pad it for us, because we keep records in the record buffer
 * array and each new record must be aligned on the 8-byte boundary,
 * since its first element is an 8-byte timestamp. Instead of letting
 * the compiler insert the padding silently, we pad explicitly, so that
 * whoever writes the binary decoder can refer to this struct to find
 * out the record size.
 */
struct __wt_track_record {
	uint64_t timestamp;
	uint64_t op_id;
	uint16_t op_type;
	char padding[6];
};

#define	WT_TRACK_OP(s, optype)						\
	tr->timestamp = __wt_rdtsc(s);					\
	tr->op_type = optype;						\
	tr->op_id = (uint64_t)&__func__;				\
									\
	if (optype == 0 && !id_recorded)				\
		__wt_optrack_record_funcid(s, tr->op_id,		\
					   (void*)&__func__,		\
					   sizeof(__func__),		\
					   &id_recorded);		\
	if (s->optrackbuf_ptr == WT_OPTRACK_MAXRECS) {			\
		if (s->optrack_fh != NULL) {				\
			size_t bytes = __wt_optrack_flush_buffer(s);	\
			s->optrack_offset += bytes;			\
		}                                                       \
		s->optrackbuf_ptr = 0;					\
	}

#define	WT_TRACK_OP_INIT(s)						\
	WT_TRACK_RECORD *tr = &(s->optrack_buf[s->optrackbuf_ptr++]);	\
	static volatile bool id_recorded = 0;				\
	WT_TRACK_OP(s, 0);

#define	WT_TRACK_OP_END(s)						\
	tr = &(s->optrack_buf[s->optrackbuf_ptr++]);			\
	WT_TRACK_OP(s, 1);
