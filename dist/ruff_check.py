#!/usr/bin/env python3

import pathlib
from shutil import which
import subprocess
import sys

# Always use this scripts folder as the working directory.
current_dir = pathlib.Path(__file__).parent.resolve()

def run(cmd):
    try:
        output = subprocess.check_output(cmd, cwd=current_dir).decode().strip()
        if output == "All checks passed!":
            return True
        else:
            print(output)
            return False
    except subprocess.CalledProcessError as cpe:
        print("The command [%s] failed:\n%s" % (' '.join(cmd), cpe.output.decode('utf-8')))
        return False

if not which("ruff"):
    doc_link = "https://docs.astral.sh/ruff/installation/"
    print("Ruff is not installed! Please execute `pip install ruff` to install it.")
    print(f"Fore more information: {doc_link}")
    exit(1)

cmd = ["ruff", "check", "--fix", "../.", "."]

if not run(cmd):
    sys.exit(1)
