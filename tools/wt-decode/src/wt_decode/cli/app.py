"""Root CLI application for the ``wtd`` command.

Registers three subcommand groups:
    decode  - Offline decode of local WiredTiger binary data
    disagg  - Online operations against a disaggregated page service
    config  - Tool configuration
"""

import typer

from wt_decode.cli.decode import decode_app
from wt_decode.cli.disagg import disagg_app
from wt_decode.cli.config import config_app

app = typer.Typer(
    help="WiredTiger Decode Tool",
    no_args_is_help=True,
)

app.add_typer(decode_app, name="decode")
app.add_typer(disagg_app, name="disagg")
app.add_typer(config_app, name="config")


def main():
    app()


if __name__ == "__main__":
    main()
