import sys
import logging
from pathlib import Path
from rich import print as rprint
from wt_decode import config as _config

logger = logging.getLogger(__name__)

def ensure_stubs_generated():
    """
    Check if gRPC stubs exist. If not, attempt to generate them.
    This runs at module load time to ensure imports work.
    """
    # Check relative to this file's parent (assuming standard layout)
    # Correcting the path to be relative to the package root where this is imported
    # This file is in src/wt_decode/disagg/utils.py, so parent is src/wt_decode/disagg
    package_dir = Path(__file__).resolve().parent
    stub_file = package_dir / "pageservice/v1/page_service_pb2.py"

    if stub_file.exists():
        return

    rprint("[yellow][*] gRPC stubs not found. Attempting to generate...[/yellow]")

    proto_dir = None
    proto_paths = _config.get_path_list("proto_paths")
    for p in proto_paths:
        if p.exists():
            proto_dir = p
            break

    if not proto_dir:
        # If we can't find it automatically, we might be in a state where we can't generate them.
        # But we'll try to let the user know.
        rprint("[red][!] Could not find proto directory. Please ensure protos are available at standard paths.[/red]")
        return

    output_dir = package_dir
    rprint(f"[blue][*] Generating Python gRPC stubs from: {proto_dir}[/blue]")

    try:
        import grpc_tools
        import grpc_tools.protoc
    except ImportError:
        rprint("[red][!] grpcio-tools not installed. Cannot generate stubs.[/red]")
        return

    grpc_protos_include = Path(grpc_tools.__file__).parent / "_proto"

    proto_files = [
        "ds/v1/ds_service_common.proto",
        "pageservice/v1/page_service.proto",
    ]

    args = [
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"-I{grpc_protos_include}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
    ] + proto_files

    if grpc_tools.protoc.main(args) != 0:
        rprint("[red][!] Failed to generate stubs.[/red]")
        return

    # Create __init__.py files for packages
    for subdir in ["ds", "ds/v1", "pageservice", "pageservice/v1"]:
        init_file = output_dir / subdir / "__init__.py"
        init_file.parent.mkdir(parents=True, exist_ok=True)
        init_file.touch(exist_ok=True)

    rprint("[green][*] Stubs generated successfully.[/green]")


def find_pagedecryptor() -> str:
    """Matches the discovery logic in wt-decode-ts."""
    pagedecryptor_paths = _config.get_path_list("pagedecryptor_paths")

    for p in pagedecryptor_paths:
        if p.exists():
            return str(p)
    return "pagedecryptor"
