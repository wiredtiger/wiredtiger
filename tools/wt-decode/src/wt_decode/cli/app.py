import argparse
import datetime
import logging
import sys
from typing import Optional

import typer
from rich import print as rprint
from rich.table import Table
from rich.panel import Panel
from rich.console import Console

from wt_decode import config as _config

console = Console()

app = typer.Typer(help="WiredTiger Disaggregated Storage Decode Tool")
dump_app = typer.Typer(help="Dump table contents")
app.add_typer(dump_app, name="dump")

logger = logging.getLogger(__name__)


def setup_debug_logging(log_path: Optional[str] = None) -> str:
    """Configure root logger for debug mode.

    Attaches a StreamHandler at DEBUG level so every log record is printed to
    stderr, and a FileHandler that writes the full trace to *log_path* (auto-
    generated when *log_path* is None).

    Returns the resolved log file path.
    """
    if log_path is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_path = f"wtd-debug-{timestamp}.log"

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # -- stderr stream handler (shows debug output live) ---------------------
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    # -- file handler (persists the full trace) -------------------------------
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    return log_path

# ---------------------------------------------------------------------------
# Config-aware default factories for CLI options
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

@app.command()
def disagg_browser(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="Initial LSN to start from (optional)"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode: print every command and log everything to a file"),
    debug_log: Optional[str] = typer.Option(None, "--debug-log", help="Path for the debug log file (default: wtd-debug-<timestamp>.log)"),
):
    """
    Interactive browser for WiredTiger disaggregated storage.
    """
    if debug:
        log_path = setup_debug_logging(debug_log)
        rprint(f"[yellow][debug] Debug mode enabled. Logging to: {log_path}[/yellow]")
        logger.debug(
            "disagg_browser started: log_id=%d page_server=%s decryptor=%s key_file=%s lsn=%s",
            log_id, page_server, decryptor_path, key_file, lsn,
        )

    from wt_decode.disagg.client import DisaggClient
    from wt_decode.cli.browse import DisaggBrowser

    with DisaggClient(page_server, decryptor_path, debug=debug) as client:
        browser = DisaggBrowser(client, key_file, log_id, debug=debug)
        browser.run(lsn)

@app.command()
def fetch_tree(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    table_id: int = typer.Option(..., help="WiredTiger table ID"),
    root_page_id: int = typer.Option(..., help="Root page ID to start traversal"),
    root_lsn: int = typer.Option(..., help="LSN of the root page"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    output_dir: Optional[str] = typer.Option(None, help="Output directory"),
    max_pages: int = typer.Option(0, help="Safety limit on pages to visit"),
    verbose: bool = typer.Option(True, help="Print cell data"),
    bson: bool = typer.Option(False, help="Decode cell values as BSON"),
    rich: bool = typer.Option(False, help="Use rich formatting for output"),
):
    """
    Fetch, decrypt, decode, and traverse a full disaggregated page tree.
    """
    from wt_decode.disagg import tree as disagg_fetch_full_tree

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
        debug=False,
        log_level="INFO",
        rich=rich
    )

    disagg_fetch_full_tree.traverse_tree(args)


@app.command()
def page(
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
    """
    Fetch, decrypt, and decode a single page.
    """
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

    # --- Full decoded output ---
    if rich:
        rich_print_page(table_id, page_id, lsn, log_id, decoded)
    else:
        console.print("\n[bold blue]--- Decoded Page Output ---[/bold blue]")
        _print_page(decoded, split=getattr(opts, 'split', False),
                    decode_as_bson=getattr(opts, 'bson', False),
                    disagg=getattr(opts, 'disagg', True))


@app.command()
def delta_chain(
    table_id: int = typer.Option(..., help="WiredTiger table ID"),
    page_id: int = typer.Option(..., help="Page ID to inspect"),
    lsn: int = typer.Option(..., help="LSN of the page version"),
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="PageService gRPC server address"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    show_history: bool = typer.Option(False, "--history", help="Also show full page version history via test service"),
):
    """
    Summarise the delta chain structure for a page at a given LSN.

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
        rprint("[dim]No deltas — this page version is a single full image.[/dim]")
    else:
        rprint(f"\n[bold]Chain summary:[/bold] 1 full image + {len(deltas)} delta(s)")
        rprint(f"  base_lsn: {page_proto.base_lsn}")

@dump_app.command("metadata")
def dump_metadata(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="LSN of the turtle page to start from (optional)"),
    values_only: bool = typer.Option(False, "--values-only", help="Only dump values, omit keys"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (default: stdout)"),
):
    """
    Dump the contents of the WiredTiger metadata table (table ID 10).
    Dumps cells with key and value types in leaf pages.
    """
    from wt_decode.disagg.client import DisaggClient
    from wt_decode.cli.browse import DisaggBrowser
    from wt_decode.disagg.dump import _dump_table_values

    metadata_table_id = _config.get("metadata_table_id")
    turtle_table_id = _config.get("turtle_table_id")
    turtle_page_id = _config.get("turtle_page_id")

    with DisaggClient(page_server, decryptor_path) as client:
        browser = DisaggBrowser(client, key_file, log_id)
        if not lsn:
            try:
                history = client.get_page_history(log_id, turtle_table_id, turtle_page_id)
                if not history.metadata:
                    rprint("[red][!] No history found for turtle page.[/red]")
                    raise typer.Exit(code=1)
                lsn = history.metadata[-1].lsn
            except Exception as e:
                rprint(f"[red][!] Failed to get history: {e}[/red]")
                raise typer.Exit(code=1)

        meta_root = browser.get_metadata_root(lsn)
        if not meta_root:
            raise typer.Exit(code=1)

        _dump_table_values(client, key_file, log_id, metadata_table_id, meta_root['page_id'], meta_root['lsn'], bson=False, values_only=values_only, output_path=output)


@dump_app.command("file")
def dump_file(
    uri: str = typer.Argument(..., help="URI of the file to dump (e.g. file:collection-...)"),
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="LSN of the turtle page to start from (optional)"),
    bson: bool = typer.Option(False, "--bson", help="Decode cell values as BSON"),
    values_only: bool = typer.Option(False, "--values-only", help="Only dump values, omit keys"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (default: stdout)"),
):
    """
    Dump the contents of a specific file (table) by URI.
    Dumps cells with key and value types in leaf pages.
    """
    from wt_decode.core import btree
    from wt_decode.disagg.client import DisaggClient
    from wt_decode.cli.browse import DisaggBrowser
    from wt_decode.disagg.dump import _dump_table_values

    turtle_table_id = _config.get("turtle_table_id")
    turtle_page_id = _config.get("turtle_page_id")

    with DisaggClient(page_server, decryptor_path) as client:
        browser = DisaggBrowser(client, key_file, log_id)
        if not lsn:
            try:
                history = client.get_page_history(log_id, turtle_table_id, turtle_page_id)
                if not history.metadata:
                    rprint("[red][!] No history found for turtle page.[/red]")
                    raise typer.Exit(code=1)
                lsn = history.metadata[-1].lsn
            except Exception as e:
                rprint(f"[red][!] Failed to get history: {e}[/red]")
                raise typer.Exit(code=1)

        meta_root = browser.get_metadata_root(lsn)
        if not meta_root:
            raise typer.Exit(code=1)

        browser.load_tables_from_metadata(meta_root)
        if uri not in browser.tables:
            rprint(f"[red][!] URI {uri} not found in metadata.[/red]")
            raise typer.Exit(code=1)

        info = browser.tables[uri]
        addr = btree.DisaggAddr.parse(bytes.fromhex(info['addr_hex']))

        _dump_table_values(client, key_file, log_id, info['table_id'], addr.page_id, addr.lsn, bson=bson, values_only=values_only, output_path=output)


@app.command()
def config_show():
    """
    Show the active configuration defaults and their source.
    """
    path = _config.loaded_path()
    if path:
        rprint(f"[green]User configuration loaded from:[/green] {path}")
    else:
        rprint("[yellow]No user configuration file found. Using built-in defaults.[/yellow]")
        rprint("[dim]Searched for .wtd.toml in current dir, ~/.config/wtd/config.toml, or ~/.wtd.toml[/dim]")

    defaults = _config.all_defaults()
    if defaults:
        tbl = Table(title="Active Defaults", show_header=True)
        tbl.add_column("Key", style="cyan")
        tbl.add_column("Value")
        for k, v in sorted(defaults.items()):
            tbl.add_row(k, str(v))
        console.print(tbl)
    else:
        rprint("[red][!] No defaults found (even built-in). This is unexpected.[/red]")


def main():
    app()

if __name__ == "__main__":
    main()
