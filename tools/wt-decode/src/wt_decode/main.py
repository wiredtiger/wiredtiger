import argparse
import csv
import io
import json
import logging
from pathlib import Path
from typing import Optional

import grpc
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from .utils import ensure_stubs_generated, find_pagedecryptor
from .constants import (
    DEFAULT_KEY_FILE,
    DEFAULT_PAGE_SERVER,
    METADATA_TABLE_ID,
    TURTLE_TABLE_ID,
    TURTLE_PAGE_ID,
)

# Ensure stubs are generated before importing modules that depend on them
ensure_stubs_generated()

from py_common import btree_format
from . import config as _config
from . import disagg_fetch_full_tree
from .client import DisaggClient, create_page_service_stub, fetch_page
from .decoding import make_decode_opts, decode_page_bytes, get_page_type_name, extract_children
from .browser import DisaggBrowser
from .dump import _dump_table_values

app = typer.Typer(help="WiredTiger Disaggregated Storage Decode Tool")
console = Console()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config-aware default factories for CLI options
# ---------------------------------------------------------------------------

def _cfg_page_server() -> str:
    return _config.get("page_server", DEFAULT_PAGE_SERVER)

def _cfg_key_file() -> str:
    return _config.get("key_file", str(DEFAULT_KEY_FILE))

def _cfg_log_id() -> int:
    return _config.get("log_id", 1)

def _cfg_decryptor() -> str:
    path = _config.get("decryptor_path")
    return path if path else find_pagedecryptor()

@app.command()
def disagg_browser(
    log_id: int = typer.Option(default_factory=_cfg_log_id, help="SLS log ID (shard)"),
    page_server: str = typer.Option(default_factory=_cfg_page_server, help="Address of the PageService gRPC server"),
    decryptor_path: str = typer.Option(default_factory=_cfg_decryptor, help="Path to the pagedecryptor binary"),
    key_file: str = typer.Option(default_factory=_cfg_key_file, help="Path to the encryption key file"),
    lsn: Optional[int] = typer.Option(None, help="Initial LSN to start from (optional)"),
):
    """
    Interactive browser for WiredTiger disaggregated storage.
    """
    with DisaggClient(page_server, decryptor_path) as client:
        browser = DisaggBrowser(client, key_file, log_id)
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
def inspect_page(
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
    Fetch, decrypt, and decode a single page. Quick spot-check without full tree traversal.
    """
    from .client import decrypt_full_response_json
    from .ui import rich_print_page

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
    page_type = get_page_type_name(decoded)

    # --- Decoded header ---
    hdr_table = Table(show_header=False, box=None, padding=(0, 2))
    hdr_table.add_column("Key", style="bold green")
    hdr_table.add_column("Value")
    hdr_table.add_row("page_type", page_type)
    if decoded.page_header is not None:
        hdr_table.add_row("write_gen", str(decoded.page_header.write_gen))
        hdr_table.add_row("mem_size", str(decoded.page_header.mem_size))
        if hasattr(decoded.page_header, "ncells"):
            hdr_table.add_row("ncells", str(decoded.page_header.ncells))
    hdr_table.add_row("decrypted_size", f"{len(page_bytes)} bytes")
    console.print(Panel(hdr_table, title="[bold]Decoded Header[/bold]"))

    # --- Children (internal pages) ---
    children = extract_children(decoded)
    if children:
        child_table = Table(title="Children")
        child_table.add_column("#", style="dim")
        child_table.add_column("page_id", style="cyan")
        child_table.add_column("lsn", style="green")
        child_table.add_column("flags")
        child_table.add_column("size", justify="right")
        child_table.add_column("checksum")
        for i, child in enumerate(children):
            child_table.add_row(
                str(i), str(child["page_id"]), str(child["lsn"]),
                str(child.get("flags", "")), str(child.get("size", "")),
                str(child.get("checksum", "")),
            )
        console.print(child_table)

    # --- Full decoded output ---
    if rich:
        rich_print_page(table_id, page_id, lsn, log_id, decoded)
    else:
        console.print("\n[bold blue]--- Decoded Page Output ---[/bold blue]")
        decoded.print_page(opts)


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
    Visualize the delta chain structure for a page at a given LSN.

    Shows the full image and all deltas with their LSNs, backlink LSNs,
    sizes, write generations, and checksums.
    """
    from .client import decrypt_full_response_json

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

@app.command()
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
    Dump the contents of the WiredTiger metadata table (table ID 9).
    Dumps cells with key and value types in leaf pages.
    """
    with DisaggClient(page_server, decryptor_path) as client:
        browser = DisaggBrowser(client, key_file, log_id)
        if not lsn:
            try:
                history = client.get_page_history(log_id, TURTLE_TABLE_ID, TURTLE_PAGE_ID)
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
        
        _dump_table_values(client, key_file, log_id, METADATA_TABLE_ID, meta_root['page_id'], meta_root['lsn'], bson=False, values_only=values_only, output_path=output)


@app.command()
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
    with DisaggClient(page_server, decryptor_path) as client:
        browser = DisaggBrowser(client, key_file, log_id)
        if not lsn:
            try:
                history = client.get_page_history(log_id, TURTLE_TABLE_ID, TURTLE_PAGE_ID)
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
        addr = btree_format.DisaggAddr.parse(bytes.fromhex(info['addr_hex']))
        
        _dump_table_values(client, key_file, log_id, info['table_id'], addr.page_id, addr.lsn, bson=bson, values_only=values_only, output_path=output)


@app.command()
def config_show():
    """
    Show the current configuration file location and active defaults.
    """
    path = _config.loaded_path()
    if path:
        rprint(f"[green]Config loaded from:[/green] {path}")
        defaults = _config.all_defaults()
        if defaults:
            tbl = Table(title="[defaults]", show_header=True)
            tbl.add_column("Key", style="cyan")
            tbl.add_column("Value")
            for k, v in sorted(defaults.items()):
                tbl.add_row(k, str(v))
            console.print(tbl)
        else:
            rprint("[dim]No [defaults] section found in config.[/dim]")
    else:
        rprint("[yellow]No config file found.[/yellow]")
        rprint("[dim]Searched:[/dim]")
        for p in _config._search_paths():
            rprint(f"  {p}")
        rprint("\n[dim]Create a .wtd.toml file with:[/dim]")
        rprint("[dim]  [defaults][/dim]")
        rprint('[dim]  page_server = "172.17.0.1:20044"[/dim]')
        rprint('[dim]  key_file = "/path/to/key"[/dim]')
        rprint("[dim]  log_id = 1[/dim]")


if __name__ == "__main__":
    app()
