"""``wtd config`` subcommand group."""

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from wt_decode import config as _config

console = Console()

config_app = typer.Typer(help="Tool configuration")


@config_app.command("show")
def config_show():
    """Show the active configuration defaults and their source."""
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
