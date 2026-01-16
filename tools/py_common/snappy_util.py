#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import traceback
from py_common import binary_data
from py_common import log

have_snappy = False

def snappy_decompress_page(b: binary_data.BinaryFile, page_header, header_length, disk_size, disk_pos, opts) -> bytearray:
    # # Optional dependency: python-snappy
    try:
        import snappy
        global have_snappy
        have_snappy = True
    except:
        # Try to install it automatically
        log.warn('python-snappy not found, attempting to install...')
        try:
            import subprocess
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'python-snappy'])
            import snappy
            have_snappy = True
            log.info('Successfully installed python-snappy')
        except Exception as e:
            log.warn(f'Failed to install python-snappy: {e}')
            log.warn('Compressed pages will not be readable.')

    if not have_snappy:
        raise ModuleNotFoundError('python-snappy is required to decode compressed pages')
    try:
        compress_skip = 64
        # The first few bytes are uncompressed
        payload_data = bytearray(b.read(compress_skip - header_length))
        # Read the length of the remaining data
        compressed_byte_count = b.read_uint64()
        calculated_length = disk_size - compress_skip - 8
        lengths_match = (compressed_byte_count == calculated_length)

        # Read the maximum possible amount of compressed data
        compressed_data_full = b.read(max(calculated_length, compressed_byte_count))
        b.seek(disk_pos + disk_size)

        # Try decompression with both sizes, preferring the stored length first
        decompressed = None

        # Try stored length first (most likely to be correct)
        if compressed_byte_count <= len(compressed_data_full):
            log.info(f'Trying to decompress using stored length: {compressed_byte_count} bytes')
            compressed_data = compressed_data_full[:compressed_byte_count]
            if snappy.isValidCompressed(compressed_data):
                try:
                    decompressed = snappy.uncompress(compressed_data)
                    if not lengths_match:
                        log.info(f'  Successfully decompressed using stored length ({compressed_byte_count} bytes)')
                except:
                    pass

        # If that failed and lengths differ, try calculated length
        if decompressed is None and not lengths_match and calculated_length <= len(compressed_data_full):
            log.info(f'Trying to decompress using calculated length: {calculated_length} bytes')
            compressed_data = compressed_data_full[:calculated_length]
            if snappy.isValidCompressed(compressed_data):
                try:
                    decompressed = snappy.uncompress(compressed_data)
                    log.info(f'  Successfully decompressed using calculated length ({calculated_length} bytes)')
                except:
                    pass

        # If any attempt succeeded, use the result
        if decompressed is not None:
            payload_data.extend(decompressed)
        else:
            # Both failed - print diagnostics and stop processing this block
            # Use the stored length for diagnostics as it's more likely to be correct
            compressed_data = compressed_data_full[:min(compressed_byte_count, len(compressed_data_full))]
            print_snappy_diagnostics(compressed_data, compressed_byte_count, page_header, compress_skip)
            return
    except:
        log.error('? The page failed to uncompress')
        if opts.debug:
            traceback.print_exception(*sys.exc_info())
        return
    
    return payload_data

def decode_snappy_varint(data):
    """
    Decode the uncompressed length from snappy-compressed data.
    Snappy uses a variable-length encoding for the uncompressed size at the start.
    Returns (uncompressed_length, bytes_used) or (None, 0) if invalid.
    """
    if len(data) == 0:
        return (None, 0)

    result = 0
    shift = 0
    for i, byte in enumerate(data[:5]):  # Varint is max 5 bytes for 32-bit length
        result |= (byte & 0x7f) << shift
        if (byte & 0x80) == 0:
            return (result, i + 1)
        shift += 7
    return (None, 0)  # Invalid varint

def print_snappy_diagnostics(compressed_data, stored_length, pagehead, compress_skip):
    """Print detailed diagnostics about invalid compressed data."""
    import snappy
    log.error('? Decompression of the block failed, analyzing compressed data:')
    log.info(f'??  Compressed data length: {len(compressed_data)} bytes')
    log.info(f'??  Stored length from WiredTiger prefix: {stored_length} (0x{stored_length:x})')

    # Analyze the snappy header
    uncompressed_len, varint_bytes = decode_snappy_varint(compressed_data)
    if uncompressed_len:
        log.info(f'??  Snappy header claims: {uncompressed_len} bytes uncompressed (varint: {varint_bytes} bytes)')
        expected_uncompressed = pagehead.mem_size - compress_skip
        log.info(f'??  Page header expects: {expected_uncompressed} bytes uncompressed')

        if abs(uncompressed_len - expected_uncompressed) > 100:
            log.warn(f'??  WARNING: size mismatch of {abs(uncompressed_len - expected_uncompressed)} bytes!')
    else:
        log.error(f'??  ERROR: could not decode snappy varint header')

    # Try the full decompression first to get detailed error message
    try:
        snappy.uncompress(compressed_data)
        # If we get here, decompression succeeded (shouldn't happen if we're in diagnostics)
        log.warn(f'??  WARNING: Full decompression unexpectedly succeeded')
    except snappy.UncompressError as e:
        # Try to extract the underlying error message from the exception chain
        error_details = ""
        if e.__cause__ is not None:
            error_details = str(e.__cause__)
        elif e.__context__ is not None:
            error_details = str(e.__context__)
        else:
            # Fallback to the exception itself
            error_details = str(e)

        # Parse error message for corruption details
        import re
        dst_match = re.search(r'dst position:\s*(\d+)', error_details)
        offset_match = re.search(r'offset\s+(\d+)', error_details)

        if dst_match:
            dst_pos = int(dst_match.group(1))
            log.info(f'??  Error at output position: {dst_pos} bytes')
            if uncompressed_len:
                percent = (dst_pos / uncompressed_len) * 100
                log.info(f'??  Successfully decompressed: {dst_pos} / {uncompressed_len} bytes ({percent:.1f}%)')
            else:
                log.info(f'??  Successfully decompressed: {dst_pos} bytes before failure')

        if offset_match:
            bad_offset = int(offset_match.group(1))
            log.error(f'??  Invalid backreference: Snappy tried to copy from output offset {bad_offset}')
            if dst_match:
                if bad_offset > dst_pos:
                    log.error(f'??  ERROR: backreference offset {bad_offset} exceeds decompressed data {dst_pos} by {bad_offset - dst_pos} bytes')
                log.info(f'??  Corruption occurred in compressed stream while decompressing bytes 0-{dst_pos}')

            # Note: The backreference offset may exceed compressed data length - that's expected
            # because it refers to a position in the OUTPUT buffer, not the input stream
            if bad_offset > len(compressed_data):
                log.info(f'??  Note: backreference offset ({bad_offset}) > compressed size ({len(compressed_data)}) is expected')
                log.info(f'??       (offset refers to output buffer position, not input position)')

        if error_details and not (dst_match or offset_match):
            log.info(f'??  Error details: {error_details}')

    # Show first bytes for debugging
    if len(compressed_data) >= 32:
        log.info(f'??  first 32 bytes: {compressed_data[:32].hex(" ")}')
