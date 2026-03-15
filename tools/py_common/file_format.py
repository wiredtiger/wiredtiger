import logging

from py_common import binary_data, btree_format
from py_common.printer import Printer
from py_common.stats import PageStats


logger = logging.getLogger(__name__)


def file_header_decode(p, b):
    # block.h
    h = btree_format.BlockFileHeader.parse(b)
    logger.info('magic: ' + str(h.magic))
    logger.info('major: ' + str(h.major))
    logger.info('minor: ' + str(h.minor))
    logger.info('checksum: ' + str(h.checksum))
    if h.magic != btree_format.BlockFileHeader.WT_BLOCK_MAGIC:
        logger.info('bad magic number')
        return False
    if h.major != btree_format.BlockFileHeader.WT_BLOCK_MAJOR_VERSION:
        logger.info('bad major number')
        return False
    if h.minor != btree_format.BlockFileHeader.WT_BLOCK_MINOR_VERSION:
        logger.info('bad minor number')
        return False
    if h.unused != 0:
        logger.info('garbage in unused bytes')
        return False
    return True


def outfile_header(opts):
    if opts.output != None:
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
        opts.output.write(",".join(fields))

def wtdecode_file_object(b, opts, nbytes):
    p = Printer(b, opts)
    pagecount = 0
    if opts.offset == 0:
        valid_header = file_header_decode(p, b)
        if valid_header:
            startblock = (b.tell() + 0x1ff) & ~(0x1FF)
        else:
            logger.info("Malformed or missing file header, treating as fragment.")
            startblock = opts.offset
    else:
        startblock = opts.offset

    outfile_header(opts)

    while (nbytes == 0 or startblock < nbytes) and (opts.pages == 0 or pagecount < opts.pages):
        d_h = binary_data.d_and_h(startblock)
        PageStats.outfile_stats_start(opts, d_h)
        print('Decode at ' + d_h)
        b.seek(startblock)
        try:
            page = btree_format.WTPage.parse(b, nbytes, opts)
            if page.success:
                page.print_page(opts)
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
