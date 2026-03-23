"""``wtd disagg`` subcommand group.

Online operations against a WiredTiger disaggregated page service via gRPC.
"""

import argparse
import datetime
import logging
import sys
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from wt_decode import config as _config

console = Console()
logger = logging.getLogger(__name__)

disagg_app = typer.Typer(help="Disaggregated storage operations (requires a page service)")


# ---------------------------------------------------------------------------
# Config-aware default factories for shared options
# ---------------------------------------------------------------------------

def _cfg_page_server() -> str:
    return _config.get("page_server")

def _cfg_key_file() -> str:
    return str(_config.get_path("key_file"))

def _cfg_log_id() -> int:
    return _config.get("log_id", 1)

def _cfg_decryptor() -> str:
    path = _config.get("decryptor_path")
    if path:
        return path
    from wt_decode.disagg.utils import find_pagedecryptor
    return find_pagedecryptor()


def setup_debug_logging(log_path: Optional[str] = None) -> str:
    """Configure root logger for debug mode."""
    if log_path is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = f"wtd-debug-{timestamp}.log"

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    return log_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_turtle_lsn(client, log_id: int, lsn: Optional[int]) -> int:
    """Return *lsn* if provided, otherwise query the turtle page history
    and return the newest LSN.  Raises ``typer.Exit`` on failure."""
    if lsn:
        return lsn

    turtle_table_id = _config.get("turtle_table_id")
    turtle_page_id = _config.get("turtle_page_id")
    try:
        history = client.get_page_history(log_id, turtle_table_id, turtle_page_id)
        if not history.metadata:
            rprint("[red][!] No history found for turtle page.[/red]")
            raise typer.Exit(code=1)
        return history.metadata[-1].lsn
    except typer.Exit:
        raise
    except Exception as e:
        rprint(f"[red][!] Failed to get history: {e}[/red]")
        raise typer.Exit(code=1)


def _resolve_uri(client, key_file: str, log_id: int, uri: str, lsn: Optional[int]):
    """Resolve a file URI to (table_id, page_id, lsn) via the metadata table."""
    from wt_decode.core import btree
    from wt_decode.cli.browse import DisaggBrowser

    lsn = _resolve_turtle_lsn(client, log_id, lsn)

    browser = DisaggBrowser(client, key_file, log_id)
    meta_root = browser.get_metadata_root(lsn)
    if not meta_root:
        raise typer.Exit(code=1)

    browser.load_tables_from_metadata(meta_root)
    if uri not in browser.tables:
        rprint(f"[red][!] URI {uri} not found in metadata.[/red]")
        raise typer.Exit(code=1)

    info = browser.tables[uri]
    addr = btree.DisaggAddr.parse(bytes.fromhex(info["addr_hex"]))
    return info["table_id"], addr.page_id, addr.lsn


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@disagg_app.command("browse")
def browse(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="Initial LSN to start from (optional)"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode"),
    debug_log: Optional[str] = typer.Option(None, "--debug-log", help="Path for the debug log file"),
):
    """Interactive browser for WiredTiger disaggregated storage."""
    if debug:
        log_path = setup_debug_logging(debug_log)
        rprint(f"[yellow][debug] Debug mode enabled. Logging to: {log_path}[/yellow]")

    from wt_decode.disagg.client import DisaggClient
    from wt_decode.cli.browse import DisaggBrowser

    with DisaggClient(page_server, decryptor_path, debug=debug) as client:
        browser = DisaggBrowser(client, key_file, log_id, debug=debug)
        browser.run(lsn)


@disagg_app.command("file")
def disagg_file(
    uri: str = typer.Argument(..., help="URI of the file to inspect (e.g. file:collection-...)"),
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="LSN of the turtle page to start from (optional)"),
    data_only: bool = typer.Option(False, "--data-only", help="Only dump leaf key/value data (no tree artifacts)"),
    bson: bool = typer.Option(False, "--bson", help="Decode cell values as BSON"),
    values_only: bool = typer.Option(False, "--values-only", help="Only dump values, omit keys (with --data-only)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (default: stdout)"),
    output_dir: Optional[str] = typer.Option(None, "--output-dir", help="Output directory for tree artifacts"),
    max_pages: int = typer.Option(0, "--max-pages", help="Safety limit on pages to visit"),
    verbose: bool = typer.Option(True, help="Print cell data"),
    rich: bool = typer.Option(False, "--rich", help="Use rich formatting for output"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode"),
    debug_log: Optional[str] = typer.Option(None, "--debug-log", help="Path for the debug log file"),
):
    """Traverse a table's page tree by URI.

    By default performs a full diagnostic traversal, saving raw pages,
    decrypted data, decoded text, and a manifest to an output directory.

    With --data-only, just dumps the leaf key/value data (like the old
    ``dump file`` command).
    """
    if debug:
        log_path = setup_debug_logging(debug_log)
        rprint(f"[yellow][debug] Debug mode enabled. Logging to: {log_path}[/yellow]")

    from wt_decode.disagg.client import DisaggClient

    with DisaggClient(page_server, decryptor_path, debug=debug) as client:
        table_id, root_page_id, root_lsn = _resolve_uri(client, key_file, log_id, uri, lsn)

        if data_only:
            from wt_decode.disagg.dump import _dump_table_values
            _dump_table_values(
                client, key_file, log_id, table_id, root_page_id, root_lsn,
                bson=bson, values_only=values_only, output_path=output,
            )
        else:
            from wt_decode.disagg import tree as disagg_tree

            args = argparse.Namespace(
                log_id=log_id,
                table_id=table_id,
                root_page_id=root_page_id,
                root_lsn=root_lsn,
                page_server=page_server,
                decryptor_path=decryptor_path,
                key_file=key_file,
                verbose=verbose,
                bson=bson,
                output_dir=output_dir,
                max_pages=max_pages,
                debug=debug,
                log_level="INFO",
                rich=rich,
            )
            disagg_tree.traverse_tree(args)


@disagg_app.command("metadata")
def disagg_metadata(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="LSN of the turtle page to start from (optional)"),
    values_only: bool = typer.Option(False, "--values-only", help="Only dump values, omit keys"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (default: stdout)"),
):
    """Dump the contents of the WiredTiger metadata table."""
    from wt_decode.disagg.client import DisaggClient
    from wt_decode.cli.browse import DisaggBrowser
    from wt_decode.disagg.dump import _dump_table_values

    metadata_table_id = _config.get("metadata_table_id")

    with DisaggClient(page_server, decryptor_path) as client:
        resolved_lsn = _resolve_turtle_lsn(client, log_id, lsn)

        browser = DisaggBrowser(client, key_file, log_id)
        meta_root = browser.get_metadata_root(resolved_lsn)
        if not meta_root:
            raise typer.Exit(code=1)

        _dump_table_values(
            client, key_file, log_id, metadata_table_id,
            meta_root["page_id"], meta_root["lsn"],
            bson=False, values_only=values_only, output_path=output,
        )


@disagg_app.command("page")
def disagg_page(
    table_id: int = typer.Option(..., help="WiredTiger table ID"),
    page_id: int = typer.Option(..., help="Page ID to inspect"),
    lsn: int = typer.Option(..., help="LSN of the page"),
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="PageService gRPC server address"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    verbose: bool = typer.Option(True, help="Print cell data"),
    bson: bool = typer.Option(False, help="Decode cell values as BSON"),
    rich: bool = typer.Option(False, help="Use rich formatting for output"),
):
    """Fetch, decrypt, and decode a single page."""
    import grpc
    from wt_decode.disagg.client import create_page_service_stub, fetch_page, decrypt_full_response_json
    from wt_decode.disagg.decoding import make_decode_opts, decode_page_bytes
    from wt_decode.output.text import print_page as _print_page
    from wt_decode.ui.rich_page import rich_print_page

    stub = create_page_service_stub(page_server)

    with console.status(f"Fetching page_id={page_id} lsn={lsn} ..."):
        try:
            response = fetch_page(stub, log_id, table_id, page_id, lsn)
        except grpc.RpcError as exc:
            rprint(f"[red][!] gRPC error: {exc}[/red]")
            raise typer.Exit(code=1)

    page_proto = response.page
    deltas = list(page_proto.deltas)

    # --- Proto metadata ---
    meta_table = Table(show_header=False, box=None, padding=(0, 2))
    meta_table.add_column("Key", style="bold cyan")
    meta_table.add_column("Value")
    meta_table.add_row("table_id", str(table_id))
    meta_table.add_row("page_id", str(page_id))
    meta_table.add_row("lsn", str(lsn))
    meta_table.add_row("log_id", str(log_id))
    meta_table.add_row("contents_size", f"{len(page_proto.contents)} bytes")
    meta_table.add_row("num_deltas", str(len(deltas)))
    if deltas:
        meta_table.add_row("full_image_lsn", str(page_proto.full_image_lsn))
        meta_table.add_row("full_image_backlink_lsn", str(page_proto.full_image_backlink_lsn))
        meta_table.add_row("base_lsn", str(page_proto.base_lsn))
    console.print(Panel(meta_table, title="[bold]Proto Response[/bold]"))

    # --- Decrypt & Decode ---
    with console.status("Decrypting ..."):
        page_bytes = decrypt_full_response_json(
            decryptor_path, key_file, response, lsn, table_id, page_id,
        )

    opts = make_decode_opts(verbose=verbose, bson=bson)
    decoded = decode_page_bytes(page_bytes, opts)

    if rich:
        rich_print_page(table_id, page_id, lsn, log_id, decoded)
    else:
        console.print("\n[bold blue]--- Decoded Page Output ---[/bold blue]")
        _print_page(decoded, split=getattr(opts, "split", False),
                    decode_as_bson=getattr(opts, "bson", False),
                    disagg=getattr(opts, "disagg", True))


@disagg_app.command("delta-chain")
def disagg_delta_chain(
    table_id: int = typer.Option(..., help="WiredTiger table ID"),
    page_id: int = typer.Option(..., help="Page ID to inspect"),
    lsn: int = typer.Option(..., help="LSN of the page version"),
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="PageService gRPC server address"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    show_history: bool = typer.Option(False, "--history", help="Also show full page version history via test service"),
):
    """Summarise the delta chain structure for a page at a given LSN.

    Shows the full image and all deltas with their LSNs, backlink LSNs,
    sizes, write generations, and checksums.
    """
    import grpc
    from wt_decode.disagg.client import DisaggClient, create_page_service_stub, fetch_page, decrypt_full_response_json
    from wt_decode.disagg.decoding import make_decode_opts, decode_page_bytes

    stub = create_page_service_stub(page_server)

    # --- Optional: page version history ---
    if show_history:
        try:
            with DisaggClient(page_server, decryptor_path) as client:
                history = client.get_page_history(log_id, table_id, page_id)
                if history.metadata:
                    hist_table = Table(title=f"Page Version History (table_id={table_id}, page_id={page_id})")
                    hist_table.add_column("#", style="dim")
                    hist_table.add_column("LSN", style="cyan")
                    hist_table.add_column("Flags")
                    hist_table.add_column("Content Length", justify="right")
                    for i, m in enumerate(reversed(history.metadata)):
                        style = "bold" if m.lsn == lsn else ""
                        hist_table.add_row(
                            str(i), str(m.lsn), str(m.flags),
                            str(m.content_length), style=style,
                        )
                    console.print(hist_table)
                    console.print()
                else:
                    rprint("[yellow]No history found for this page.[/yellow]")
        except Exception as e:
            rprint(f"[yellow]Could not fetch history (requires test service): {e}[/yellow]")

    # --- Fetch the page at the specified LSN ---
    with console.status(f"Fetching page_id={page_id} lsn={lsn} ..."):
        try:
            response = fetch_page(stub, log_id, table_id, page_id, lsn)
        except grpc.RpcError as exc:
            rprint(f"[red][!] gRPC error: {exc}[/red]")
            raise typer.Exit(code=1)

    page_proto = response.page
    deltas = list(page_proto.deltas)

    # --- Delta chain table ---
    chain_table = Table(
        title=f"Delta Chain (table_id={table_id}, page_id={page_id}, lsn={lsn})",
    )
    chain_table.add_column("Entry", style="bold")
    chain_table.add_column("LSN", style="cyan")
    chain_table.add_column("Backlink LSN", style="yellow")
    chain_table.add_column("Size", justify="right")
    chain_table.add_column("Write Gen", justify="right")
    chain_table.add_column("Checksum", justify="right")

    # --- Full image row ---
    fi_write_gen = "-"
    fi_checksum = "-"
    try:
        fi_bytes = decrypt_full_response_json(
            decryptor_path, key_file, response, lsn, table_id, page_id,
        )
        fi_opts = make_decode_opts(verbose=False)
        fi_decoded = decode_page_bytes(fi_bytes, fi_opts)
        if fi_decoded.page_header is not None:
            fi_write_gen = str(fi_decoded.page_header.write_gen)
        if fi_decoded.block_header is not None and hasattr(fi_decoded.block_header, "checksum"):
            fi_checksum = str(fi_decoded.block_header.checksum)
    except Exception:
        pass

    chain_table.add_row(
        "[green]Full Image[/green]",
        str(page_proto.full_image_lsn) if deltas else str(lsn),
        str(page_proto.full_image_backlink_lsn) if deltas else "-",
        str(len(page_proto.contents)),
        fi_write_gen,
        fi_checksum,
    )

    # --- Delta rows ---
    lsns = list(page_proto.lsns)
    backlinks = list(page_proto.backlinks)
    for i, delta_bytes in enumerate(deltas):
        delta_lsn = str(lsns[i]) if i < len(lsns) else "?"
        delta_backlink = str(backlinks[i]) if i < len(backlinks) else "?"
        chain_table.add_row(
            f"[yellow]Delta {i}[/yellow]",
            delta_lsn,
            delta_backlink,
            str(len(delta_bytes)),
            "-",
            "-",
        )

    console.print(chain_table)

    if not deltas:
        rprint("[dim]No deltas -- this page version is a single full image.[/dim]")
    else:
        rprint(f"\n[bold]Chain summary:[/bold] 1 full image + {len(deltas)} delta(s)")
        rprint(f"  base_lsn: {page_proto.base_lsn}")
