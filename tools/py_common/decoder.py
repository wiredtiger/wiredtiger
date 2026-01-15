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

from dataclasses import dataclass
import io
import json
import pprint
import sys
import traceback
from typing import Optional, List, Union

from py_common import btree_format, binary_data
from py_common.stats import PageStats
from py_common.snappy_util import print_snappy_diagnostics
from py_common.printer import binary_to_pretty_string, raw_bytes, Printer, dumpraw

@dataclass
class WTPage:
    """
    Representation of a decoded WT page. 
    
    """
    
    success: bool = False
    
    page_header: Optional[btree_format.PageHeader] = None
    block_header: Optional[Union[btree_format.BlockHeader, btree_format.BlockDisaggHeader]] = None
    cells: Optional[List[btree_format.Cell]] = None
    extents: Optional[List[btree_format.ExtentItem]] = None
    
    @staticmethod
    def parse(b: binary_data.BinaryFile, nbytes: int, opts) -> 'WTPage':
        
        page = WTPage(success=False)
                
        disk_pos = b.tell()

        if opts.disagg:
            # Size of WT_PAGE_HEADER
            page_data = bytearray(b.read(44))
        else:
            # Size of WT_PAGE_HEADER + size of WT_BLOCK_HEADER
            page_data = bytearray(b.read(40))
        b.saved_bytes()
        b_page = binary_data.BinaryFile(io.BytesIO(page_data))
        
        p = Printer(b_page, opts)

        # WT_PAGE_HEADER in btmem.h (28 bytes)
        page.page_header = btree_format.PageHeader.parse(b_page)
        # WT_BLOCK_HEADER in block.h (12 bytes or 44 bytes)
        if opts.disagg:
            page.block_header = btree_format.BlockDisaggHeader.parse(b_page)
        else:
            page.block_header = btree_format.BlockHeader.parse(b_page)

        if page.page_header.unused != 0:
            p.rint('? garbage in unused bytes')
            return page
        if page.page_header.type == btree_format.PageType.WT_PAGE_INVALID:
            p.rint('? invalid page')
            return page

        p.rint(page.page_header)

        if page.block_header.unused != 0:
            p.rint('garbage in unused bytes')
            return page

        disk_size = nbytes if opts.disagg else page.block_header.disk_size

        if disk_size > 17 * 1024 * 1024:
            # The maximum document size in MongoDB is 16MB. Larger block sizes are suspect.
            p.rint('the block is too big')
            return page
        if disk_size < 40 and not opts.disagg:
            # The disk size is too small
            return page

        p.rint(page.block_header)

        pagestats = PageStats()

        # Optional dependency: crc32c
        have_crc32c = False
        try:
            import crc32c
            have_crc32c = True
        except:
            pass

        # Verify the checksum
        if have_crc32c:
            savepos = b.tell()
            b.seek(disk_pos)
            if (opts.disagg and page.block_header.flags & btree_format.BlockDisaggFlags.WT_BLOCK_DISAGG_DATA_CKSUM) \
                or (not opts.disagg and page.block_header.flags & btree_format.BlockFlags.WT_BLOCK_DATA_CKSUM):
                check_size = disk_size
            else:
                check_size = 64
            data = bytearray(b.read(check_size))
            b.seek(savepos)
            # Zero-out the checksum field
            data[32] = data[33] = data[34] = data[35] = 0
            if len(data) < check_size:
                p.rint('? reached EOF before the end of the block')
                return page
            checksum = crc32c.crc32c(data)
            if checksum != page.block_header.checksum:
                p.rint(f'? the calculated checksum {hex(checksum)} does not match header checksum {page.block_header.checksum}')
                if (not opts.cont):
                    return page

        # Skip the rest if we don't want to display the data
        skip_data = opts.skip_data

        if skip_data:
            b.seek(disk_pos + disk_size)
            page.success = True
            return page

        # Read the block contents
        payload_pos = b.tell()
        header_length = payload_pos - disk_pos
        if page.page_header.flags & btree_format.PageFlags.WT_PAGE_COMPRESSED:
            # Optional dependency: python-snappy
            have_snappy = False
            try:
                import snappy
                have_snappy = True
            except:
                # Try to install it automatically
                print('python-snappy not found, attempting to install...')
                try:
                    import subprocess
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'python-snappy'])
                    import snappy
                    have_snappy = True
                    print('Successfully installed python-snappy')
                except Exception as e:
                    print(f'Warning: Failed to install python-snappy: {e}')
                    print('Compressed pages will not be readable.')
        
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
                    p.rint_v(f'Trying to decompress using stored length: {compressed_byte_count} bytes')
                    compressed_data = compressed_data_full[:compressed_byte_count]
                    if snappy.isValidCompressed(compressed_data):
                        try:
                            decompressed = snappy.uncompress(compressed_data)
                            if not lengths_match:
                                p.rint_v(f'  Successfully decompressed using stored length ({compressed_byte_count} bytes)')
                        except:
                            pass

                # If that failed and lengths differ, try calculated length
                if decompressed is None and not lengths_match and calculated_length <= len(compressed_data_full):
                    p.rint_v(f'Trying to decompress using calculated length: {calculated_length} bytes')
                    compressed_data = compressed_data_full[:calculated_length]
                    if snappy.isValidCompressed(compressed_data):
                        try:
                            decompressed = snappy.uncompress(compressed_data)
                            p.rint_v(f'  Successfully decompressed using calculated length ({calculated_length} bytes)')
                        except:
                            pass

                # If any attempt succeeded, use the result
                if decompressed is not None:
                    payload_data.extend(decompressed)
                else:
                    # Both failed - print diagnostics and stop processing this block
                    # Use the stored length for diagnostics as it's more likely to be correct
                    compressed_data = compressed_data_full[:min(compressed_byte_count, len(compressed_data_full))]
                    print_snappy_diagnostics(p, compressed_data, compressed_byte_count, page.page_header, compress_skip)
                    return page  # Stop processing this corrupted block
            except:
                p.rint('? The page failed to uncompress')
                if opts.debug:
                    traceback.print_exception(*sys.exc_info())
                return page
        else:
            payload_data = b.read(page.page_header.mem_size - header_length)
            b.seek(disk_pos + disk_size)

        # Add the payload to the page data & reinitialize the stream and the printer
        page_data.extend(payload_data)
        b_page = binary_data.BinaryFile(io.BytesIO(page_data))
        b_page.seek(header_length)
        p = Printer(b_page, opts)

        # Parse the block contents
        if page.page_header.type == btree_format.PageType.WT_PAGE_INVALID:
            pass    # a blank page: TODO maybe should check that it's all zeros?
        elif page.page_header.type == btree_format.PageType.WT_PAGE_BLOCK_MANAGER:
            extents = page.decode_extlist(b_page, p)
            page.extents = extents
        elif page.page_header.type == btree_format.PageType.WT_PAGE_ROW_INT or \
            page.page_header.type == btree_format.PageType.WT_PAGE_ROW_LEAF:
            cells = page.decode_rows(b_page, p, opts, pagestats)
            page.cells = cells
        elif page.page_header.type == btree_format.PageType.WT_PAGE_OVFL:
            # Use b_page.read() so that we can also print the raw bytes in the split mode
            p.rint_v(raw_bytes(b_page.read(len(payload_data))))
        else:
            p.rint_v('? unimplemented decode for page type {}'.format(page.page_header.type))
            p.rint_v(binary_to_pretty_string(payload_data))

        PageStats.outfile_stats_end(opts, page.page_header, page.block_header, pagestats)
        page.success = True
        return page
        
    def decode_rows(self, b, p, opts, pagestats) -> List[btree_format.Cell]:
        cells = []
        for cellnum in range(0, self.page_header.entries):
            cellpos = b.tell()
            if cellpos >= self.page_header.mem_size:
                p.rint_v('** OVERFLOW memsize **')
                return cells
            p.begin_cell(cellnum)

            try:
                cell = btree_format.Cell.parse(b, True)
                cells.append(cell)
                
                p.rint_v(cell.descriptor_string())
                if cell.has_timestamps():
                    cell.process_timestamps(p, pagestats)

                if cell.is_key:
                    pagestats.num_keys += 1
                    pagestats.keys_sz += len(cell.data)
                
                # If the cell cannot be decoded as a valid type, dump the raw bytes and raise an error.
                if not cell.is_valid_type():
                    dumpraw(p, b, cellpos)
                    raise ValueError('Unexpected cell type')

                p.rint_v(cell.type_string())
                
                # Print the contents of the cell.
                try:
                    # Optional dependency: bson
                    have_bson = False
                    try:
                        import bson
                        have_bson = True
                    except:
                        pass
                    # Attempt the decode the cell as BSON.
                    if (cell.is_value and opts.bson and have_bson):
                        decoded_data = bson.BSON(cell.data).decode()
                        p.rint_v(pprint.pformat(decoded_data, indent=2))
                    # If the cell is an address and we're in disagg mode, print the cell as a DisaggAddr
                    # type.
                    elif cell.is_address and opts.disagg:
                        addr = btree_format.DisaggAddr.parse(cell.data)
                        p.rint(json.dumps(addr.__dict__))
                    else:
                        p.rint_v(raw_bytes(cell.data))
                except bson.InvalidBSON as e:
                    p.rint_v(f"cannot decode cell as BSON: {e}")
                    p.rint_v(raw_bytes(cell.data))
                except (IndexError, ValueError):
                    # FIXME-WT-13000 theres a bug in raw_bytes
                    pass

            finally:
                p.end_cell()
        
        return cells
        
    def decode_extlist(self, b, p) -> List[btree_format.ExtentItem]:
        # Written by block_ext.c
        extents = []
        okay = True
        cellnum = -1
        lastoff = 0
        p.rint_ext('extent list follows:')
        while True:
            cellnum += 1
            cellpos = b.tell()
            if cellpos >= self.page_header.mem_size:
                p.rint_ext(f'** OVERFLOW memsize ** memsize={self.page_header.mem_size}, position={cellpos}')
                return extents
            p.begin_cell(cellnum)

            try:
                extent = btree_format.ExtentItem.parse(b)
                extents.append(extent)
                extra_stuff = ''
                
                if cellnum == 0:
                    extra_stuff += '  # magic number'
                    if not extent.is_magic():
                        extra_stuff = f'  # ERROR: magic number did not match expected value=' + \
                            f'{btree_format.ExtentItem.WT_BLOCK_EXTLIST_MAGIC}'
                        okay = False
                else:
                    if extent.offset < lastoff:
                        extra_stuff = f'  # ERROR: list out of order'
                        okay = False

                    # We expect sizes and positions to be multiples of
                    # this number, it is conservative.
                    multiple = 256
                    if extent.offset % multiple != 0:
                        extra_stuff = f'  # ERROR: offset is not a multiple of {multiple}'
                        okay = False
                    if extent.offset != 0 and extent.size % multiple != 0:
                        extra_stuff = f'  # ERROR: size is not a multiple of {multiple}'
                        okay = False

                # A zero offset is written as an end of list marker,
                # in that case, the size is a version number.
                # For version 0, this is truly the end of the list.
                # For version 1, additional entries may be appended to this (avail) list.
                #
                # See __wti_block_extlist_write() in block_ext.c, and calls
                # to that function in block_ckpt.c.
                if extent.is_end_of_list():
                    extra_stuff += '  # end of list'
                    if extent.size == 0:
                        extra_stuff += ', version 0'
                    elif extent.size == 1:
                        extra_stuff += ', version 1,' + \
                        ' any following entries are not yet in this (incomplete) checkpoint'
                    else:
                        extra_stuff += f' -- ERROR unexpected size={extent.size} has no meaning here'
                        okay = False
                
                p.rint_ext(f'  {extent.offset}, {extent.size}{extra_stuff}')
                if not extent.is_magic():
                    lastoff = extent.offset
            finally:
                p.end_cell()
            if extent.is_end_of_list() or not okay:
                break
        
        return extents
    