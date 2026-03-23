"""
Minimal decode entry point using only the core lib.

Usage:
    python -m wt_decode <file>              Decode a .wt file
    python -m wt_decode --hex <file>        Decode hex dump input
    python -m wt_decode --format json -     Read from stdin, emit JSON
"""

import argparse
import logging
import os
import sys
from contextlib import nullcontext

from wt_decode.core import binary
from wt_decode.core.options import DecodeOptions
from wt_decode.core.file_decoder import wtdecode_file_object
from wt_decode.core.log_parser import process_logs
from wt_decode.core.sqlite_reader import is_sqlite3_file, process_sqlite_file


def open_input_file(filename, mode):
    if filename == '-':
        stream = sys.stdin if 'b' not in mode else sys.stdin.buffer
        return nullcontext(stream)
    return open(filename, mode)


def main():
    parser = argparse.ArgumentParser(
        description='Decode WiredTiger binary data',
        prog='python -m wt_decode',
    )
    parser.add_argument('filename', help="input file or '-' for stdin")

    inargs = parser.add_argument_group('input options')
    inargs.add_argument('--hex', action='store_true',
        help='input is hex dump (may be embedded in log messages)')
    inargs.add_argument('--bson', action='store_true',
        help='decode cell values as BSON data')
    inargs.add_argument('--disagg-table', action='store_true',
        help='input is a full disagg table from GetTableAtLSN endpoint')
    inargs.add_argument('--disagg', action='store_true',
        help='input comes from disaggregated storage')
    inargs.add_argument('-o', '--offset', type=int, default=0,
        help='seek offset before decoding')
    inargs.add_argument('--page-id', type=int, default=None,
        help='(sqlite) decode only this page_id')
    inargs.add_argument('--lsn', type=int, default=None,
        help='(sqlite) decode only this LSN')
    inargs.add_argument('-p', '--pages', type=int, default=0,
        help='number of pages to decode')
    inargs.add_argument('--skip-data', action='store_true',
        help='do not read/process cell data')
    inargs.add_argument('--keyfile', type=str,
        help='keyfile path for encryption')

    outargs = parser.add_argument_group('output options')
    outargs.add_argument('-v', '--verbose', action='count', default=0,
        help='verbose logging output (repeat for more: -v, -vv)')
    outargs.add_argument('-b', '--bytes', action='store_true',
        help='show bytes alongside decoding')
    outargs.add_argument('-c', '--csv', type=str, dest='output', action='store',
        help='output filename for CSV statistics')
    outargs.add_argument('--continue', dest='cont', action='store_true',
        help='continue on checksum failure')
    outargs.add_argument('-s', '--split', action='store_true',
        help='split output to also show raw bytes')
    outargs.add_argument('--format', choices=['text', 'json', 'jsonl'],
        default='text', help='output format (default: text)')

    args = parser.parse_args()

    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = log_levels[min(args.verbose, len(log_levels) - 1)]
    logging.basicConfig(level=level, format='[%(levelname)s] %(message)s')

    csv_file = open(args.output, 'w') if args.output else None

    try:
        opts = DecodeOptions(
            dumpin=args.hex,
            disagg_table=args.disagg_table,
            disagg=args.disagg,
            skip_data=args.skip_data,
            cont=args.cont,
            split=args.split,
            bson=args.bson,
            output=csv_file,
            offset=args.offset,
            pages=args.pages,
            keyfile=getattr(args, 'keyfile', None),
            lsn=args.lsn,
            page_id=args.page_id,
        )

        filename = args.filename

        if args.format in ('json', 'jsonl'):
            from wt_decode.output.json import JsonFormatter, JsonlFormatter
            # JSON output mode — decode pages and emit structured output
            if opts.dumpin:
                # For hex/log input, fall through to text mode for now
                pass
            # For now, fall through to text mode — JSON integration with the
            # file_decoder loop will be wired up in a later phase.

        if opts.dumpin:
            with open_input_file(filename, 'r') as infile:
                process_logs(infile, opts)
        elif opts.disagg_table:
            with open_input_file(filename, 'r') as infile:
                from wt_decode.disagg.page_service import process_disagg_table
                process_disagg_table(infile, opts)
        elif is_sqlite3_file(filename):
            process_sqlite_file(filename, opts)
        else:
            nbytes = 0 if filename == '-' else os.path.getsize(filename)
            input_name = 'stdin' if filename == '-' else filename
            input_size = 'unknown' if filename == '-' else hex(nbytes)
            print(f'{input_name}, position {hex(opts.offset)}, size {input_size}, '
                    f'pagelimit {opts.pages}')
            with open_input_file(filename, 'rb') as infile:
                wtdecode_file_object(
                    binary.BinaryFile(infile), nbytes, opts)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        if csv_file:
            csv_file.close()


if __name__ == '__main__':
    main()
