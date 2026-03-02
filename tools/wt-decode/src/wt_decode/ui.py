import json
from typing import List, Optional
from rich.console import Console, Group
from rich.table import Table
from rich.panel import Panel

from prompt_toolkit.shortcuts import prompt
from prompt_toolkit.completion import FuzzyWordCompleter
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML

from py_common import binary_data, btree_format
from .decoding import get_page_type_name

console = Console()

# Maps the protobuf UpdateType enum int to a short human-readable label.
_UPDATE_TYPE_NAMES = {
    0: "DELTA",
    1: "FULL",
    2: "TOMBSTONE",
    3: "CKPT_END",
}


def update_type_name(flags_value) -> str:
    """Return a readable name for a PageMetadata flags / UpdateType value."""
    v = int(flags_value)
    return _UPDATE_TYPE_NAMES.get(v, f"UNKNOWN({v})")


def build_page_history_choices(metadata_list):
    """Build questionary choices from PageMetadata entries formatted as aligned table rows.

    Returns a list of questionary.Choice / questionary.Separator objects suitable
    for passing straight to ``questionary.select(..., choices=...)``.

    *metadata_list* should already be in the desired display order (e.g.
    newest-first via ``reversed(history.metadata)``).
    """
    import questionary

    entries = list(metadata_list)
    if not entries:
        return []

    # Pre-compute column widths for alignment.
    lsn_w = max(len(str(m.lsn)) for m in entries)
    type_w = max(len(update_type_name(m.flags)) for m in entries)
    size_w = max(len(str(m.content_length)) for m in entries)
    idx_w = len(str(len(entries) - 1))

    def _fmt(idx, m, *, label=""):
        type_name = update_type_name(m.flags)
        parts = [
            f"{'#' + str(idx):<{idx_w + 1}}",
            f"LSN {str(m.lsn):>{lsn_w}}",
            f"{type_name:<{type_w}}",
            f"{m.content_length:>{size_w}}B",
        ]
        extras = []
        if m.is_compressed:
            extras.append("compressed")
        if m.is_encrypted:
            extras.append("encrypted")
        if m.backlink_lsn:
            extras.append(f"backlink={m.backlink_lsn}")
        if m.base_lsn:
            extras.append(f"base={m.base_lsn}")
        if extras:
            parts.append("  ".join(extras))
        if label:
            parts.append(label)
        return "  ".join(parts)

    # Header separator
    header = (
        f"{'#':<{idx_w + 1}}  "
        f"{'LSN':>{lsn_w + 4}}  "
        f"{'Type':<{type_w}}  "
        f"{'Size':>{size_w + 1}}"
    )

    choices = [questionary.Separator(f"─ {header} ─")]
    for i, m in enumerate(entries):
        label = "← newest" if i == 0 else ("← oldest" if i == len(entries) - 1 else "")
        choices.append(
            questionary.Choice(
                title=_fmt(i, m, label=label),
                value=m.lsn,
            )
        )

    return choices

def fuzzy_select(message: str, choices: List[str]) -> Optional[str]:
    """Display a filterable list of choices using prompt_toolkit.
    
    Filters as the user types and allows selection via arrows/enter.
    Shows the menu immediately even if input is empty. Empty input returns None.
    """
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.shortcuts import PromptSession
    from prompt_toolkit.styles import Style
    from prompt_toolkit.formatted_text import HTML
    from prompt_toolkit.validation import Validator, ValidationError

    # WordCompleter with match_middle=True gives a similar effect to fuzzy for many cases
    # but more importantly, it shows all results on empty string by default.
    completer = WordCompleter(choices, match_middle=True, ignore_case=True)
    
    # Custom style to make the completion menu look good
    style = Style.from_dict({
        'completion-menu.completion': 'bg:#222222 #ffffff',
        'completion-menu.completion.current': 'bg:#00aaaa #000000',
        'scrollbar.background': 'bg:#444444',
        'scrollbar.button': 'bg:#888888',
    })

    class ChoiceValidator(Validator):
        def validate(self, document):
            if document.text and document.text not in choices:
                raise ValidationError(message="Please select an item from the list or leave empty to exit.")

    try:
        session = PromptSession(completer=completer, style=style)
        # complete_while_typing=True and reserve_space_for_menu ensures the list is visible
        result = session.prompt(
            HTML(f"<b>{message}</b> (Type to filter, arrows to select, empty to exit): "),
            complete_while_typing=True,
            reserve_space_for_menu=10,
            validator=ChoiceValidator(),
        )
        return result if result else None
    except KeyboardInterrupt:
        return None
    except EOFError:
        return None

def _format_cell_data(data: bytes, is_value: bool = False) -> str:
    """Heuristic to format cell data as BSON, string, packed int, or hex."""
    if not data:
        return ""
    
    # 1. Try BSON if it might be a value
    if is_value:
        try:
            import bson
            decoded = bson.BSON(data).decode()
            return json.dumps(decoded, default=str)
        except:
            pass

    # 2. Try to see if it's a printable string
    try:
        s = data.decode('utf-8', errors='strict').rstrip('\0')
        if len(s) > 0 and all(c.isprintable() or c.isspace() for c in s):
            return f'"{s}"'
    except:
        pass

    # 3. Try to see if it's one or more packed ints
    if data[0] >= 0x7f:
        try:
            result = []
            remaining = data
            while remaining:
                val, next_remaining = binary_data.unpack_int(remaining)
                if len(next_remaining) == len(remaining):
                    break
                result.append(f"<packed {val} ({hex(val)})>")
                remaining = next_remaining
            if not remaining:
                return " ".join(result)
        except:
            pass

    return data.hex()

def rich_print_page(table_id: int, page_id: int, lsn: int, log_id: int, page: btree_format.WTPage):
    """User friendly display of WiredTiger pages using Rich."""
    sections = []

    # 1. Disagg Metadata
    meta_table = Table(show_header=False, box=None, padding=(0, 2))
    meta_table.add_column("Key", style="bold cyan")
    meta_table.add_column("Value")
    meta_table.add_row("Table ID", str(table_id))
    meta_table.add_row("Page ID", str(page_id))
    meta_table.add_row("LSN", str(lsn))
    meta_table.add_row("Log ID", str(log_id))
    sections.append(Panel(meta_table, title="[bold]Disagg Metadata[/bold]", border_style="cyan"))

    # 2. Page Header
    if page.page_header:
        hdr_table = Table(show_header=False, box=None, padding=(0, 2))
        hdr_table.add_column("Key", style="bold green")
        hdr_table.add_column("Value")
        hdr_table.add_row("Page Type", f"{page.page_header.type.value} ({get_page_type_name(page)})")
        hdr_table.add_row("Recno", str(page.page_header.recno))
        hdr_table.add_row("Write Gen", str(page.page_header.write_gen))
        hdr_table.add_row("Mem Size", str(page.page_header.mem_size))
        hdr_table.add_row("NCells (overflow len)", str(page.page_header.entries))
        hdr_table.add_row("Page Flags", f"0x{page.page_header.flags:x} ({str(page.page_header.flags)})")
        hdr_table.add_row("Version", str(page.page_header.version))
        sections.append(Panel(hdr_table, title="[bold]Page Header[/bold]", border_style="green"))

    # 3. Block Disagg Header
    if page.block_header:
        title = "Block Header"
        border_style = "yellow"
        blk_table = Table(show_header=False, box=None, padding=(0, 2))
        blk_table.add_column("Key", style="bold yellow")
        blk_table.add_column("Value")
        
        if isinstance(page.block_header, btree_format.BlockDisaggHeader):
            title = "Block Disagg Header"
            blk_table.add_row("Magic", hex(page.block_header.magic))
            blk_table.add_row("Version", str(page.block_header.version))
            blk_table.add_row("Compatible Version", str(page.block_header.compatible_version))
            blk_table.add_row("Header Size", str(page.block_header.header_size))
            blk_table.add_row("Checksum", str(page.block_header.checksum))
            blk_table.add_row("Previous Checksum", str(page.block_header.previous_checksum))
            blk_table.add_row("Flags", f"0x{page.block_header.flags:x} ({str(page.block_header.flags)})")
        else:
            if hasattr(page.block_header, "disk_size"):
                blk_table.add_row("Disk Size", str(page.block_header.disk_size))
            blk_table.add_row("Checksum", str(page.block_header.checksum))
            blk_table.add_row("Flags", f"0x{page.block_header.flags:x}")
        
        sections.append(Panel(blk_table, title=f"[bold]{title}[/bold]", border_style=border_style))

    # 4. Cells
    if page.cells:
        cell_table = Table(title="Cells", expand=True)
        cell_table.add_column("#", justify="right", style="dim")
        cell_table.add_column("Type", style="bold")
        cell_table.add_column("Desc/Extra", style="magenta")
        cell_table.add_column("Details", style="blue")
        cell_table.add_column("Data (Preview)", overflow="fold")
        
        for i, cell in enumerate(page.cells):
            ctype = "Unknown"
            if cell.is_address: ctype = "Address"
            elif cell.is_key: ctype = "Key"
            elif cell.is_value: ctype = "Value"
            
            if cell.is_short:
                ctype = f"Short {ctype}"

            desc_extra = f"desc: 0x{cell.descriptor:x}"
            if cell.extra_descriptor:
                desc_extra += f"\nextra: 0x{cell.extra_descriptor:x}"
            
            details_parts = []
            if cell.prefix_compression_count:
                details_parts.append(f"pfx: {cell.prefix_compression_count}")
            if cell.run_length is not None:
                details_parts.append(f"runlen/addr: {binary_data.d_and_h(cell.run_length)}")
            
            # Timestamps
            if cell.start_ts: details_parts.append(f"start ts: {hex(cell.start_ts)}")
            if cell.start_txn: details_parts.append(f"start txn: {hex(cell.start_txn)}")
            if cell.stop_ts: details_parts.append(f"stop ts: {hex(cell.stop_ts)}")
            if cell.stop_txn: details_parts.append(f"stop txn: {hex(cell.stop_txn)}")
            
            details = "\n".join(details_parts)

            if cell.is_address:
                try:
                    addr = btree_format.DisaggAddr.parse(cell.data)
                    data_preview = f"page_id={addr.page_id}, lsn={addr.lsn}"
                except Exception:
                    data_preview = _format_cell_data(cell.data, is_value=False)
            else:
                data_preview = _format_cell_data(cell.data, is_value=cell.is_value)

            if len(data_preview) > 200:
                data_preview = data_preview[:197] + "..."
            
            cell_table.add_row(str(i), ctype, desc_extra, details, data_preview)
        
        sections.append(cell_table)

    console.print(Panel(Group(*sections), title=f"[bold white]WiredTiger Page (Table {table_id}, Page {page_id})[/bold white]", border_style="blue"))
