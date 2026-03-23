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

import json
import logging
import pprint

from wt_decode.core import binary
from wt_decode.core.binary import binary_to_pretty_string

logger = logging.getLogger(__name__)

# Manages printing to output.
# We keep track of cells, the first line printed for a new cell
# shows the cell number, subsequent lines are indented a little.
# If the split option is on, we show any bytes that were used
# in decoding before the regular decoding output appears.
# Those 'input bytes' are shown shifted to the right.
class Printer(object):
    def __init__(self, binfile, *, split=False):
        self.binfile = binfile
        self.issplit = split
        self.cellpfx = ''
        self.in_cell = False

    def begin_cell(self, cell_number):
        self.cellpfx = f'{cell_number}: '
        self.in_cell = True
        ignore = self.binfile.saved_bytes()  # reset the saved position

    def end_cell(self):
        self.in_cell = False
        self.cellpfx = ''

    # This is the 'print' function, used as p.rint()
    def rint(self, s):
        if self.issplit:
            saved_bytes = self.binfile.saved_bytes()[:]
            # For the split view, we want to have the bytes related to
            # stuff to be normally printed to appear indented by 40 spaces,
            # with 10 more spaces to show a possibly abbreviated file position.
            # If we are beginning a cell, we want that to appear left justified,
            # within the 40 spaces of indentation.
            if len(saved_bytes) > 0:
                # create the 10 character file position
                # the current file position has actually advanced by
                # some number of bytes, so subtract that now.
                cur_pos = self.binfile.tell() - len(saved_bytes)
                file_pos = f'{cur_pos:x}'
                if len(file_pos) > 8:
                    file_pos = '...' + file_pos[-5:]
                elif len(file_pos) < 8:
                    file_pos = ' ' * (8 - len(file_pos)) + file_pos
                file_pos += ': '

                indentation = (self.cellpfx + ' ' * 40)[0:40]
                self.cellpfx = ''
                while len(saved_bytes) > 20:
                    print(indentation + file_pos + str(saved_bytes[:20].hex(' ')))
                    saved_bytes = saved_bytes[20:]
                    indentation = ' ' * 40
                    file_pos = ' ' * 10
                print(indentation + file_pos + str(saved_bytes.hex(' ')))

        pfx = self.cellpfx
        self.cellpfx = ''
        if pfx == '' and self.in_cell:
            pfx = '  '
        print(pfx + str(s))

def raw_bytes(b):
    if type(b) != type(b''):
        # Not bytes, it's already a string.
        return b

    # If the high bit of the first byte is on, it's likely we have
    # a packed integer.  If the high bit is off, it's possible we have
    # a packed integer (it would be negative) but it's harder to guess,
    # we'll presume a string.  But if the byte is 0x7f, that's ASCII DEL,
    # very unlikely to be the beginning of a string, but it decodes as -1,
    # so seems more likely to be an int.  If the UTF-8 decoding of the
    # string fails, we probably just have binary data.

    # Try decoding as one or more packed ints
    result = ''
    s = b
    while len(s) > 0 and s[0] >= 0x7f:
        try:
            val, next_s = binary.unpack_int(s)
            if result != '':
                result += ' '
            result += f'<packed {binary.d_and_h(val)}>'
            s = next_s
        except (ValueError, IndexError):
            break
    if len(s) == 0:
        return result

    # See if the rest of the bytes can be decoded as a string
    try:
        if result != '':
            result += ' '
        return f'"{result + s.decode()}"'
    except:
        pass

    # The earlier steps failed, so it must be binary data
    return binary_to_pretty_string(b, start_with_line_prefix=False)


################################################################
# Page / cell printing functions
################################################################

try:
    import bson
    _HAVE_BSON = True
except ImportError:
    bson = None
    _HAVE_BSON = False


def print_page(page, *, split: bool = False,
               decode_as_bson: bool = False, disagg: bool = False):
    """Print a decoded WTPage to stdout."""
    from wt_decode.core import btree as _btree

    p = Printer(page.raw_bytes, split=split)
    p.rint(page.page_header)
    p.rint(page.block_header)

    if page.page_header.type == _btree.PageType.WT_PAGE_INVALID:
        pass
    elif page.page_header.type == _btree.PageType.WT_PAGE_BLOCK_MANAGER:
        if page.extents is not None:
            print_extents(page, p)
    elif page.page_header.type in (_btree.PageType.WT_PAGE_ROW_INT, _btree.PageType.WT_PAGE_ROW_LEAF):
        if page.cells is not None:
            _print_cells(page, p, _btree=_btree, decode_as_bson=decode_as_bson, disagg=disagg)
    elif page.page_header.type == _btree.PageType.WT_PAGE_OVFL:
        if page.page_header.entries > 0:
            overflow_data = page.raw_bytes.read(page.page_header.entries)
            p.rint(raw_bytes(overflow_data))
    else:
        logger.warning(f'? unimplemented decode for page type {page.page_header.type}')


def _print_cells(page, p, *, _btree, decode_as_bson: bool = False, disagg: bool = False):
    """Print all cells in a page."""
    for cellnum, cell in enumerate(page.cells):
        p.begin_cell(cellnum)
        p.rint(cell.descriptor_string())
        p.rint(cell.type_string())
        print_cell_timestamps(cell, p)

        try:
            if cell.is_value and decode_as_bson and _HAVE_BSON:
                decoded_data = bson.BSON(cell.data).decode()
                p.rint(pprint.pformat(decoded_data, indent=2))
            elif cell.is_address and disagg:
                addr = _btree.DisaggAddr.parse(cell.data)
                p.rint(json.dumps(addr.__dict__))
            else:
                p.rint(raw_bytes(cell.data))
        except (IndexError, ValueError):
            # FIXME-WT-13000
            pass
        except Exception as e:
            if _HAVE_BSON and isinstance(e, bson.InvalidBSON):
                p.rint(f"cannot decode cell as BSON: {e}")
                p.rint(raw_bytes(cell.data))
            else:
                raise

        p.end_cell()


def print_extents(page, p):
    """Print all extents in a block manager page."""
    p.rint('extent list follows:')
    for extnum, extent in enumerate(page.extents):
        p.begin_cell(extnum)
        p.rint(f'  {extent.offset}, {extent.size}{extent.extra_stuff}')


def print_cell_timestamps(cell, p):
    """Print timestamp information for a cell."""
    if cell.extra_descriptor == 0:
        return

    p.rint('cell has timestamps:')
    if cell.prepared:
        p.rint(' prepared')

    if cell.start_ts is not None:
        p.rint(' start ts: ' + binary.ts(cell.start_ts))
    if cell.start_txn is not None:
        p.rint(' start txn: ' + binary.txn(cell.start_txn))
    if cell.durable_start_ts is not None:
        p.rint(' durable start ts: ' + binary.ts(cell.durable_start_ts))

    if cell.stop_ts is not None:
        p.rint(' stop ts: ' + binary.ts(cell.stop_ts))
    if cell.stop_txn is not None:
        p.rint(' stop txn: ' + binary.txn(cell.stop_txn))
    if cell.durable_stop_ts is not None:
        p.rint(' durable stop ts: ' + binary.ts(cell.durable_stop_ts))
