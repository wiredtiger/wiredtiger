import argparse
import io
import logging
import re
from typing import Any, Optional, Dict

import questionary
from rich import print as rprint
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from py_common import binary_data, btree_format
from .client import DisaggClient
from .decoding import make_decode_opts, get_page_type_name
from .ui import rich_print_page, build_page_history_choices, fuzzy_select
from .dump import _dump_table_values
from . import disagg_fetch_full_tree
from .constants import METADATA_TABLE_ID, TURTLE_TABLE_ID, TURTLE_PAGE_ID

logger = logging.getLogger(__name__)

class DisaggBrowser:
    def __init__(self, client: DisaggClient, key_file: str, log_id: int):
        self.client = client
        self.key_file = key_file
        self.log_id = log_id
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.use_rich = True

    def run(self, initial_lsn: Optional[int] = None):
        rprint(Panel.fit("[bold cyan]WiredTiger Disaggregated Browser[/bold cyan]"))
        
        lsn = initial_lsn
        if not lsn:
            rprint(f"[yellow][*] LSN not provided, querying history for turtle page (table_id={TURTLE_TABLE_ID}, page_id={TURTLE_PAGE_ID})...[/yellow]")
            try:
                history = self.client.get_page_history(self.log_id, TURTLE_TABLE_ID, TURTLE_PAGE_ID)
                if not history.metadata:
                    rprint("[red][!] No history found for turtle page.[/red]")
                    return

                ordered = list(reversed(history.metadata))
                choices = build_page_history_choices(ordered)

                lsn = questionary.select(
                    "Select turtle page LSN to start from:",
                    choices=choices,
                    use_shortcuts=True
                ).ask()
                
                if lsn is None: return
                rprint(f"[green][*] Selected LSN: {lsn}[/green]")
            except Exception as e:
                rprint(f"[red][!] Failed to get history: {e}[/red]")
                return

        meta_root = self.get_metadata_root(lsn)
        if not meta_root: return

        self.load_tables_from_metadata(meta_root)

        if not self.tables:
            rprint("[red][!] No tables found in metadata.[/red]")
            return

        self.main_menu()

    def get_metadata_root(self, lsn: int) -> Optional[Dict[str, int]]:
        rprint(f"[blue][*] Fetching turtle page (table_id={TURTLE_TABLE_ID}, page_id={TURTLE_PAGE_ID}, lsn={lsn})...[/blue]")
        try:
            resp = self.client.get_page_at_lsn(self.log_id, TURTLE_TABLE_ID, TURTLE_PAGE_ID, lsn)
            decrypted = self.client.decrypt_full_response(self.key_file, resp, lsn, TURTLE_TABLE_ID, TURTLE_PAGE_ID)
            content = decrypted.decode('ascii', errors='ignore')

            match = re.search(r'addr="([0-9a-fA-F]+)"', content)
            if not match:
                rprint("[red][!] Could not find root address in turtle page.[/red]")
                return None

            addr_hex = match.group(1)
            addr = btree_format.DisaggAddr.parse(bytes.fromhex(addr_hex))
            rprint(f"[green][*] Metadata table root found: page_id={addr.page_id}, lsn={addr.lsn}[/green]")
            return {"page_id": addr.page_id, "lsn": addr.lsn}
        except Exception as e:
            rprint(f"[red][!] Failed to fetch/decrypt turtle page: {e}[/red]")
            return None

    def load_tables_from_metadata(self, root: Dict[str, int]):
        rprint("[blue][*] Fetching metadata table...[/blue]")
        
        metadata_table_id = METADATA_TABLE_ID
        queue = [root]
        visited = set()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Scanning metadata...", total=None)
            
            while queue:
                current = queue.pop(0)
                key = f"{current['page_id']}:{current['lsn']}"
                if key in visited: continue
                visited.add(key)

                try:
                    resp = self.client.get_page_at_lsn(self.log_id, metadata_table_id, current['page_id'], current['lsn'])
                    decrypted = self.client.decrypt_full_response(self.key_file, resp, current['lsn'], metadata_table_id, current['page_id'])
                    
                    b = binary_data.BinaryFile(io.BytesIO(decrypted))
                    decode_opts = make_decode_opts()
                    page = btree_format.WTPage()
                    page = page.parse(b, len(decrypted), decode_opts)
                    
                    if page.page_header.type == btree_format.PageType.WT_PAGE_ROW_INT:
                        for cell in page.cells:
                            if cell.is_address:
                                addr = btree_format.DisaggAddr.parse(cell.data)
                                queue.append({"page_id": addr.page_id, "lsn": addr.lsn})
                    elif page.page_header.type == btree_format.PageType.WT_PAGE_ROW_LEAF:
                        for i in range(len(page.cells) - 1):
                            key_cell = page.cells[i]
                            val_cell = page.cells[i+1]
                            if key_cell.is_key and val_cell.is_value:
                                key_str = key_cell.data.decode('ascii', errors='ignore').strip('\0').strip()
                                val_str = val_cell.data.decode('ascii', errors='ignore').strip('\0').strip()
                                
                                if key_str.startswith('file:'):
                                    id_match = re.search(r'\bid=(\d+)', val_str)
                                    addr_match = re.search(r'addr="([0-9a-fA-F]+)"', val_str)
                                    if id_match and addr_match:
                                        self.tables[key_str] = {
                                            "table_id": int(id_match.group(1)),
                                            "addr_hex": addr_match.group(1)
                                        }
                except Exception as e:
                    logger.debug("Failed to process metadata page page_id=%s lsn=%s: %s",
                                 current['page_id'], current['lsn'], e)
        
        rprint(f"[green][*] Found {len(self.tables)} tables in metadata.[/green]")

    def main_menu(self):
        table_names = sorted(self.tables.keys())
        choices = ["Dump Metadata Table"] + table_names + ["Exit"]
        
        while True:
            selected_table = fuzzy_select("Select a table to inspect", choices)

            if selected_table == "Exit" or selected_table is None: break
            if selected_table == "Dump Metadata Table":
                self.dump_metadata_interactive()
                continue
            self.table_menu(selected_table)

    def table_menu(self, table_name: str):
        info = self.tables[table_name]
        addr = btree_format.DisaggAddr.parse(bytes.fromhex(info['addr_hex']))

        while True:
            rprint(f"\n[bold yellow]Table: {table_name}[/bold yellow]")
            rprint(f"ID:    {info['table_id']}")
            rprint(f"Root:  page_id={addr.page_id}, lsn={addr.lsn}")

            action = questionary.select(
                "Select action:",
                choices=[
                    {"name": "Interactive Tree Traversal", "value": "TRAVERSE"},
                    {"name": "Fetch whole table recursively", "value": "FETCH"},
                    {"name": "Dump contents", "value": "DUMP"},
                    questionary.Separator(),
                    {"name": "Back to table list", "value": "BACK"}
                ]
            ).ask()

            if action == "BACK" or action is None: break
            if action == "TRAVERSE":
                self.traverse_tree(info['table_id'], addr.page_id, addr.lsn)
            elif action == "FETCH":
                self.fetch_table_interactive(table_name, info['table_id'], addr.page_id, addr.lsn)
            elif action == "DUMP":
                bson = questionary.confirm("Decode values as BSON?", default=False).ask()
                values_only = questionary.confirm("Values only (omit keys)?", default=False).ask()
                
                clean_name = table_name.replace("file:", "").replace(".wt", "")
                default_path = f"/tmp/{clean_name}-dump-{addr.lsn}.txt"
                output_path = questionary.text("Save output to:", default=default_path).ask()
                
                if bson is not None and values_only is not None and output_path:
                    _dump_table_values(
                        self.client, self.key_file, self.log_id, 
                        info['table_id'], addr.page_id, addr.lsn, 
                        bson=bson, values_only=values_only, output_path=output_path
                    )

    def dump_metadata_interactive(self):
        try:
            history = self.client.get_page_history(self.log_id, 1, 1)
            if not history.metadata:
                rprint("[red][!] No history found for turtle page.[/red]")
                return

            ordered = list(reversed(history.metadata))
            choices = build_page_history_choices(ordered)

            lsn = questionary.select(
                "Select metadata version (LSN) to dump:",
                choices=choices,
                use_shortcuts=True
            ).ask()
            
            if lsn is None: return

            values_only = questionary.confirm("Values only (omit keys)?", default=False).ask()
            
            default_path = f"/tmp/metadata-dump-{lsn}.txt"
            output_path = questionary.text("Save output to:", default=default_path).ask()
            
            if values_only is not None and output_path:
                meta_root = self.get_metadata_root(lsn)
                if not meta_root: return
                
                _dump_table_values(
                    self.client, self.key_file, self.log_id, 9, 
                    meta_root['page_id'], meta_root['lsn'], 
                    bson=False, values_only=values_only, output_path=output_path
                )
        except Exception as e:
            rprint(f"[red][!] Dump failed: {e}[/red]")

    def fetch_and_decode(self, table_id: int, page_id: int, lsn: int) -> Optional[btree_format.WTPage]:
        try:
            resp = self.client.get_page_at_lsn(self.log_id, table_id, page_id, lsn)
            decrypted = self.client.decrypt_full_response(self.key_file, resp, lsn, table_id, page_id)
            b = binary_data.BinaryFile(io.BytesIO(decrypted))
            decode_opts = make_decode_opts()
            page = btree_format.WTPage()
            page = page.parse(b, len(decrypted), decode_opts)
            return page
        except Exception as e:
            rprint(f"[red][!] Error: {e}[/red]")
            return None

    def traverse_tree(self, table_id: int, root_page_id: int, root_lsn: int):
        current = {"page_id": root_page_id, "lsn": root_lsn}
        stack = [] # Navigation stack: list of {page_id, lsn, siblings, sibling_index}

        while True:
            page = self.fetch_and_decode(table_id, current['page_id'], current['lsn'])
            if not page:
                if stack:
                    rprint("[yellow]Returning to parent due to error...[/yellow]")
                    last = stack.pop()
                    current = {"page_id": last['page_id'], "lsn": last['lsn']}
                    continue
                else: break

            if self.use_rich:
                rich_print_page(table_id, current['page_id'], current['lsn'], self.log_id, page)
            else:
                rprint(f"\n[bold blue]--- Page: id={table_id}, page={current['page_id']}, lsn={current['lsn']} ---[/bold blue]")
                decode_opts = make_decode_opts()
                page.print_page(decode_opts)

            children = []
            if page.cells:
                for cell in page.cells:
                    if cell.is_address:
                        addr = btree_format.DisaggAddr.parse(cell.data)
                        children.append({"page_id": addr.page_id, "lsn": addr.lsn})

            choices = []
            if children:
                choices.append(questionary.Separator("--- Children ---"))
                for i, child in enumerate(children):
                    choices.append(
                        {"name": f"Child [{i}]: page_id={child['page_id']}, lsn={child['lsn']}", 
                         "value": {"type": "CHILD", "index": i}}
                    )

            choices.append(questionary.Separator("--- Navigation ---"))
            
            mode_label = "Switch to Standard View" if self.use_rich else "Switch to Rich View"
            choices.append({"name": mode_label, "value": {"type": "TOGGLE_MODE"}})

            if stack:
                choices.append({"name": "Go Up (Parent)", "value": {"type": "UP"}})
                current_nav = stack[-1]
                if current_nav['sibling_index'] > 0:
                    choices.append({"name": "Previous Sibling", "value": {"type": "PREV"}})
                if current_nav['sibling_index'] < len(current_nav['siblings']) - 1:
                    choices.append({"name": "Next Sibling", "value": {"type": "NEXT"}})
            
            choices.append({"name": "Exit Traversal", "value": {"type": "EXIT"}})

            action = questionary.select(
                "Select action:",
                choices=choices,
                use_shortcuts=True
            ).ask()

            if action is None or action['type'] == "EXIT": break
            if action['type'] == "TOGGLE_MODE":
                self.use_rich = not self.use_rich
                continue
            if action['type'] == "UP":
                last = stack.pop()
                current = {"page_id": last['page_id'], "lsn": last['lsn']}
            elif action['type'] == "PREV":
                stack[-1]['sibling_index'] -= 1
                current = stack[-1]['siblings'][stack[-1]['sibling_index']]
            elif action['type'] == "NEXT":
                stack[-1]['sibling_index'] += 1
                current = stack[-1]['siblings'][stack[-1]['sibling_index']]
            elif action['type'] == "CHILD":
                stack.append({
                    "page_id": current['page_id'],
                    "lsn": current['lsn'],
                    "siblings": children,
                    "sibling_index": action['index']
                })
                current = children[action['index']]

    def fetch_table_interactive(self, table_name: str, table_id: int, root_page_id: int, root_lsn: int):
        default_dir = f"./fetch_{table_name.replace('file:', '').replace('.wt_stable', '')}"
        output_dir = questionary.text("Output directory:", default=default_dir).ask()
        if output_dir is None: return

        args = argparse.Namespace(
            log_id=self.log_id,
            table_id=table_id,
            root_page_id=root_page_id,
            root_lsn=root_lsn,
            page_server=self.client.server_addr,
            decryptor_path=self.client.decryptor_path,
            key_file=self.key_file,
            verbose=True,
            bson=False,
            output_dir=output_dir,
            max_pages=0,
            debug=False,
            log_level="INFO"
        )
        
        disagg_fetch_full_tree.traverse_tree(args)
