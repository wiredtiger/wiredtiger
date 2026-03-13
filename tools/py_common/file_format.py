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

import logging

from py_common import binary_data, btree_format
from py_common.decode_opts import DecodeOptions
from py_common.printer import Printer
from py_common.stats import PageStats


logger = logging.getLogger(__name__)


def file_header_decode(p, b):
    # block.h
    h = btree_format.BlockFileHeader.parse(b)
    p.rint('magic: ' + str(h.magic))
    p.rint('major: ' + str(h.major))
    p.rint('minor: ' + str(h.minor))
    p.rint('checksum: ' + str(h.checksum))
    if h.magic != btree_format.BlockFileHeader.WT_BLOCK_MAGIC:
        p.rint('bad magic number')
        return
    if h.major != btree_format.BlockFileHeader.WT_BLOCK_MAJOR_VERSION:
        p.rint('bad major number')
        return
    if h.minor != btree_format.BlockFileHeader.WT_BLOCK_MINOR_VERSION:
        p.rint('bad minor number')
        return
    if h.unused != 0:
        p.rint('garbage in unused bytes')
        return
    p.rint('')


def outfile_header(output):
    if output != None:
        fields = [
            "block id",

            # page head
            "writegen",
            "memsize",
            "ncells",
            "page type",

            # block head
            "disk size",

            # page stats
            *PageStats.csv_cols(),
        ]
        output.write(",".join(fields))

def wtdecode_file_object(b, nbytes, opts: DecodeOptions):
    p = Printer(b, split=opts.split)
    pagecount = 0
    startblock = opts.offset
    if opts.offset == 0 and not opts.fragment:
        file_header_decode(p, b)
        startblock = (b.tell() + 0x1ff) & ~(0x1FF)

    outfile_header(opts.output)

    while (nbytes == 0 or startblock < nbytes) and (opts.pages == 0 or pagecount < opts.pages):
        d_h = binary_data.d_and_h(startblock)
        PageStats.outfile_stats_start(opts.output, d_h)
        print('Decode at ' + d_h)
        b.seek(startblock)
        try:
            page = btree_format.WTPage.parse(b, nbytes,
                                             disagg=opts.disagg,
                                             skip_data=opts.skip_data,
                                             cont=opts.cont)
            if page.success:
                page.print_page(split=opts.split,
                                decode_as_bson=opts.bson,
                                disagg=opts.disagg)
                if page.pagestats:
                    PageStats.outfile_stats_end(opts.output,
                                               page.page_header,
                                               page.block_header,
                                               page.pagestats)
            p.rint('')
        except BrokenPipeError:
            break
        except ModuleNotFoundError as e:
            # We're missing snappy compression support. No point continuing from here.
            p.rint('ERROR: ' + str(e))
            exit(1)
        except Exception:
            p.rint(f'ERROR decoding block at {binary_data.d_and_h(startblock)}')
            logger.debug('Exception while decoding block', exc_info=True)
        pos = b.tell()

        # If we're in attached storage mode align the file pointer on a 512 byte boundary.
        if not opts.disagg:
            pos = (pos + 0x1FF) & ~(0x1FF)

        if startblock == pos:
            startblock += 0x200
        else:
            startblock = pos
        pagecount += 1
    p.rint('')
