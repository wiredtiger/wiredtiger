/*
 * CRC-32 algorithms implemented with the z/Architecture
 * Vector Extension Facility.
 *
 * Copyright IBM Corp. 2015
 * Author(s): Hendrik Brueckner <brueckner@linux.vnet.ibm.com>
 *
 */
#include <sys/types.h>
#include <endian.h>
#include "crc32-s390x.h"
#include "slicing-consts.h"

#define VX_MIN_LEN		64
#define VX_ALIGNMENT		16L
#define VX_ALIGN_MASK		(VX_ALIGNMENT - 1)

/* Prototypes for functions in assembly files */
unsigned int crc32_be_vgfm_16(unsigned int crc, const unsigned char *buf, size_t size);
unsigned int crc32_le_vgfm_16(unsigned int crc, const unsigned char *buf, size_t size);
unsigned int crc32c_be_vgfm_16(unsigned int crc, const unsigned char *buf, size_t size);
unsigned int crc32c_le_vgfm_16(unsigned int crc, const unsigned char *buf, size_t size);

/* Pure C implementations of CRC, one byte at a time */
unsigned int crc32_be(unsigned int crc, const unsigned char *buf, size_t len) {
	while (len--)
		crc = crc32table_be[0][((crc >> 24) ^ *buf++) & 0xFF] ^ (crc << 8);
	return crc;
}

unsigned int crc32c_be(unsigned int crc, const unsigned char *buf, size_t len) {
	while (len--)
		crc = crc32ctable_be[0][((crc >> 24) ^ *buf++) & 0xFF] ^ (crc << 8);
	return crc;
}

unsigned int crc32_le(unsigned int crc, const unsigned char *buf, size_t len) {
	crc = htole32(crc);
	while (len--)
		crc = crc32table_le[0][((crc >> 24) ^ *buf++) & 0xFF] ^ (crc << 8);
	crc = le32toh(crc);
	return crc;
}

unsigned int crc32c_le(unsigned int crc, const unsigned char *buf, size_t len){
	crc = htole32(crc);
	while (len--)
		crc = crc32ctable_le[0][((crc >> 24) ^ *buf++) & 0xFF] ^ (crc << 8);
	crc = le32toh(crc);
	return crc;
}

/*
 * DEFINE_CRC32_VX() - Define a CRC-32 function using the vector extension
 *
 * Creates a function to perform a particular CRC-32 computation. Depending
 * on the message buffer, the hardware-accelerated or software implementation
 * is used. Note that the message buffer is aligned to improve fetch
 * operations of VECTOR LOAD MULTIPLE instructions.
 *
 */
#define DEFINE_CRC32_VX(___fname, ___crc32_vx, ___crc32_sw)                 \
	unsigned int ___fname(unsigned int crc,                             \
			      const unsigned char *data,                    \
			      size_t datalen)                               \
	{                                                                   \
		unsigned long prealign, aligned, remaining;                 \
		                                                            \
		if ((unsigned long)data & VX_ALIGN_MASK) {                  \
			prealign = VX_ALIGNMENT -                           \
				   ((unsigned long)data & VX_ALIGN_MASK);   \
			datalen -= prealign;                                \
			crc = ___crc32_sw(crc, data, prealign);             \
			data = data + prealign;                             \
		}                                                           \
		                                                            \
		if (datalen < VX_MIN_LEN)                                   \
			return ___crc32_sw(crc, data, datalen);             \
		                                                            \
		aligned = datalen & ~VX_ALIGN_MASK;                         \
		remaining = datalen & VX_ALIGN_MASK;                        \
		                                                            \
		crc = ___crc32_vx(crc, data, aligned);                      \
		data = data + aligned;                                      \
		                                                            \
		if (remaining)                                              \
			crc = ___crc32_sw(crc, data, remaining);            \
		                                                            \
		return crc;                                                 \
	}

/* Main CRC-32 functions */
DEFINE_CRC32_VX(crc32_be_vx, crc32_be_vgfm_16, crc32_be)
DEFINE_CRC32_VX(crc32_le_vx, crc32_le_vgfm_16, crc32_le)
DEFINE_CRC32_VX(crc32c_be_vx, crc32c_be_vgfm_16, crc32c_be)
DEFINE_CRC32_VX(crc32c_le_vx, crc32c_le_vgfm_16, crc32c_le)
