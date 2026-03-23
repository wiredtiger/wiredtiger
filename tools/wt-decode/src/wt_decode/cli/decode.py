"""``wtd decode`` subcommand group.

Offline decoding of local WiredTiger binary data from files or stdin.
"""

import logging
import os
import sys
from contextlib import nullcontext
from typing import Optional

import typer
from rich.console import Console

from wt_decode.core.options import DecodeOptions

console = Console()
logger = logging.getLogger(__name__)

decode_app = typer.Typer(help="Decode local WiredTiger binary data")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_input(filename: str, mode: str):
    """Open *filename* for reading, or return stdin when filename is ``-``."""
    if filename == "-":
        stream = sys.stdin if "b" not in mode else sys.stdin.buffer
        return nullcontext(stream)
    return open(filename, mode)


def _log_level(verbose: int) -> int:
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    return levels[min(verbose, len(levels) - 1)]


def _make_opts(
    *,
    bson: bool = False,
    split: bool = False,
    cont: bool = False,
    skip_data: bool = False,
    offset: int = 0,
    pages: int = 0,
    csv_file=None,
    keyfile: Optional[str] = None,
    lsn: Optional[int] = None,
    page_id: Optional[int] = None,
    disagg: bool = False,
    dumpin: bool = False,
    disagg_table: bool = False,
) -> DecodeOptions:
    return DecodeOptions(
        dumpin=dumpin,
        disagg_table=disagg_table,
        disagg=disagg,
        skip_data=skip_data,
        cont=cont,
        split=split,
        bson=bson,
        output=csv_file,
        offset=offset,
        pages=pages,
        keyfile=keyfile,
        lsn=lsn,
        page_id=page_id,
    )


# ---------------------------------------------------------------------------
# Shared options — every decode command gets these
# ---------------------------------------------------------------------------

_Verbose = typer.Option(0, "--verbose", "-v", count=True, help="Verbose logging output (repeat for more: -v, -vv)")
_Bson = typer.Option(False, "--bson", help="Decode cell values as BSON")
_Split = typer.Option(False, "--split", "-s", help="Show raw bytes alongside decoded output")
_Continue = typer.Option(False, "--continue", help="Continue on checksum failure")
_SkipData = typer.Option(False, "--skip-data", help="Skip reading/processing cell data")
_Offset = typer.Option(0, "--offset", "-o", help="Byte offset to start decoding from")
_Pages = typer.Option(0, "--pages", "-p", help="Number of pages to decode (0 = unlimited)")
_Csv = typer.Option(None, "--csv", "-c", help="Output CSV statistics to this file")
_Keyfile = typer.Option(None, "--keyfile", help="Keyfile path for encryption")
_Format = typer.Option("text", "--format", help="Output format (text, json, jsonl)")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@decode_app.command("file")
def decode_file(
    path: str = typer.Argument(..., help="Path to a .wt file, or '-' for stdin"),
    verbose: int = _Verbose,
    bson: bool = _Bson,
    split: bool = _Split,
    cont: bool = _Continue,
    skip_data: bool = _SkipData,
    offset: int = _Offset,
    pages: int = _Pages,
    csv: Optional[str] = _Csv,
    keyfile: Optional[str] = _Keyfile,
    format: str = _Format,  # noqa: A002 — not yet wired to JSON output
    disagg: bool = typer.Option(False, "--disagg", help="Input comes from disaggregated storage"),
):
    """Decode a WiredTiger .wt data file (or read raw bytes from stdin)."""
    from wt_decode.core import binary
    from wt_decode.core.file_decoder import wtdecode_file_object

    _ = format  # reserved for future JSON/JSONL output support

    logging.basicConfig(level=_log_level(verbose), format="[%(levelname)s] %(message)s")
    csv_file = open(csv, "w") if csv else None

    try:
        opts = _make_opts(
            bson=bson, split=split, cont=cont, skip_data=skip_data,
            offset=offset, pages=pages, csv_file=csv_file, keyfile=keyfile,
            disagg=disagg,
        )
        nbytes = 0 if path == "-" else os.path.getsize(path)
        input_name = "stdin" if path == "-" else path
        input_size = "unknown" if path == "-" else hex(nbytes)
        print(f"{input_name}, position {hex(opts.offset)}, size {input_size}, pagelimit {opts.pages}")

        with _open_input(path, "rb") as infile:
            wtdecode_file_object(binary.BinaryFile(infile), nbytes, opts)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        if csv_file:
            csv_file.close()


@decode_app.command("hex")
def decode_hex(
    path: str = typer.Argument(..., help="Path to a log file containing hex dumps, or '-' for stdin"),
    verbose: int = _Verbose,
    bson: bool = _Bson,
    split: bool = _Split,
    cont: bool = _Continue,
    skip_data: bool = _SkipData,
    offset: int = _Offset,
    pages: int = _Pages,
    csv: Optional[str] = _Csv,
    keyfile: Optional[str] = _Keyfile,
    disagg: bool = typer.Option(False, "--disagg", help="Input comes from disaggregated storage"),
):
    """Decode hex dumps embedded in MongoDB or WiredTiger log files."""
    from wt_decode.core.log_parser import process_logs

    logging.basicConfig(level=_log_level(verbose), format="[%(levelname)s] %(message)s")
    csv_file = open(csv, "w") if csv else None

    try:
        opts = _make_opts(
            dumpin=True, bson=bson, split=split, cont=cont,
            skip_data=skip_data, offset=offset, pages=pages,
            csv_file=csv_file, keyfile=keyfile, disagg=disagg,
        )
        with _open_input(path, "r") as infile:
            process_logs(infile, opts)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        if csv_file:
            csv_file.close()


@decode_app.command("sqlite")
def decode_sqlite(
    path: str = typer.Argument(..., help="Path to a SQLite page store file"),
    verbose: int = _Verbose,
    bson: bool = _Bson,
    split: bool = _Split,
    cont: bool = _Continue,
    skip_data: bool = _SkipData,
    pages: int = _Pages,
    csv: Optional[str] = _Csv,
    keyfile: Optional[str] = _Keyfile,
    lsn: Optional[int] = typer.Option(None, "--lsn", help="Decode only this LSN"),
    page_id: Optional[int] = typer.Option(None, "--page-id", help="Decode only this page_id"),
):
    """Decode pages from a SQLite page store database."""
    from wt_decode.core.sqlite_reader import process_sqlite_file

    logging.basicConfig(level=_log_level(verbose), format="[%(levelname)s] %(message)s")
    csv_file = open(csv, "w") if csv else None

    try:
        opts = _make_opts(
            disagg=True, bson=bson, split=split, cont=cont,
            skip_data=skip_data, pages=pages, csv_file=csv_file,
            keyfile=keyfile, lsn=lsn, page_id=page_id,
        )
        process_sqlite_file(path, opts)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        if csv_file:
            csv_file.close()


@decode_app.command("table")
def decode_table(
    path: str = typer.Argument(..., help="Path to disagg table JSON from GetTableAtLSN, or '-' for stdin"),
    verbose: int = _Verbose,
    bson: bool = _Bson,
    split: bool = _Split,
    cont: bool = _Continue,
    skip_data: bool = _SkipData,
    pages: int = _Pages,
    csv: Optional[str] = _Csv,
    keyfile: Optional[str] = _Keyfile,
):
    """Decode a full disaggregated table from GetTableAtLSN JSON output."""
    from wt_decode.disagg.page_service import process_disagg_table

    logging.basicConfig(level=_log_level(verbose), format="[%(levelname)s] %(message)s")
    csv_file = open(csv, "w") if csv else None

    try:
        opts = _make_opts(
            disagg_table=True, disagg=True, bson=bson, split=split,
            cont=cont, skip_data=skip_data, pages=pages,
            csv_file=csv_file, keyfile=keyfile,
        )
        with _open_input(path, "r") as infile:
            process_disagg_table(infile, opts)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        if csv_file:
            csv_file.close()
