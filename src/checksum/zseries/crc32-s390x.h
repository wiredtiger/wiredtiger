/*
 * Copyright IBM Corp. 2015
 */
#include <sys/types.h>

/* Portable implementations of CRC-32 (IEEE and Castagnoli), both
   big-endian and little-endian variants. */
unsigned int crc32_be(unsigned int, const unsigned char *, size_t);
unsigned int crc32_le(unsigned int, const unsigned char *, size_t);
unsigned int crc32c_be(unsigned int, const unsigned char *, size_t);
unsigned int crc32c_le(unsigned int, const unsigned char *, size_t);

/* Hardware-accelerated versions of the above. It is up to the caller
   to detect the availability of vector facility and kernel support. */
unsigned int crc32_be_vx(unsigned int, const unsigned char *, size_t);
unsigned int crc32_le_vx(unsigned int, const unsigned char *, size_t);
unsigned int crc32c_be_vx(unsigned int, const unsigned char *, size_t);
unsigned int crc32c_le_vx(unsigned int, const unsigned char *, size_t);
