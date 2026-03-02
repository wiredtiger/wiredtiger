import io
import json
import logging
from typing import Optional

from rich import print as rprint

from py_common import binary_data, btree_format
from .client import DisaggClient
from .decoding import make_decode_opts

logger = logging.getLogger(__name__)

def _dump_table_values(client: DisaggClient, key_file: str, log_id: int, table_id: int, root_page_id: int, root_lsn: int, bson: bool = False, values_only: bool = False, output_path: Optional[str] = None):
    queue = [{"page_id": root_page_id, "lsn": root_lsn}]
    visited = set()
    decode_opts = make_decode_opts(verbose=True, bson=bson)
    
    bson_lib = None
    if bson:
        try:
            import bson as bson_lib
        except ImportError:
            rprint("[red][!] bson library not found. Please install pymongo.[/red]")
            bson = False

    output_f = None
    if output_path:
        try:
            output_f = open(output_path, "w")
            rprint(f"[green][*] Dumping to {output_path}...[/green]")
        except Exception as e:
            rprint(f"[red][!] Failed to open output file: {e}[/red]")
            return

    try:
        while queue:
            current = queue.pop(0)
            key = f"{current['page_id']}:{current['lsn']}"
            if key in visited:
                continue
            visited.add(key)
            
            try:
                resp = client.get_page_at_lsn(log_id, table_id, current['page_id'], current['lsn'])
                decrypted = client.decrypt_full_response(key_file, resp, current['lsn'], table_id, current['page_id'])
                
                b = binary_data.BinaryFile(io.BytesIO(decrypted))
                page = btree_format.WTPage()
                page = page.parse(b, len(decrypted), decode_opts)
                
                if page.page_header.type == btree_format.PageType.WT_PAGE_ROW_INT:
                    for cell in page.cells:
                        if cell.is_address:
                            addr = btree_format.DisaggAddr.parse(cell.data)
                            queue.append({"page_id": addr.page_id, "lsn": addr.lsn})
                elif page.page_header.type == btree_format.PageType.WT_PAGE_ROW_LEAF:
                    for cell in page.cells:
                        if cell.is_key and not values_only:
                            try:
                                s = cell.data.decode('utf-8', errors='replace').rstrip('\0')
                                line = f"[K] {s}"
                            except Exception:
                                line = f"[K] {cell.data.hex()}"
                            
                            if output_f: output_f.write(line + "\n")
                            else: print(line)

                        elif cell.is_value:
                            prefix = "[V] " if not values_only else ""
                            if bson and bson_lib:
                                try:
                                    decoded = bson_lib.BSON(cell.data).decode()
                                    line = f"{prefix}{json.dumps(decoded, default=str)}"
                                except Exception:
                                    line = f"{prefix}{cell.data.hex()}"
                            else:
                                try:
                                    s = cell.data.decode('utf-8', errors='replace').rstrip('\0')
                                    line = f"{prefix}{s}"
                                except Exception:
                                    line = f"{prefix}{cell.data.hex()}"
                            
                            if output_f: output_f.write(line + "\n")
                            else: print(line)
            except Exception as e:
                logger.debug(f"Failed to process page page_id={current['page_id']} lsn={current['lsn']}: {e}")
    finally:
        if output_f:
            output_f.close()
            rprint(f"[green][*] Dump complete.[/green]")
