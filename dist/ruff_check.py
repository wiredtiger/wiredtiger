#!/usr/bin/env python3

import subprocess
import sys

def run(cmd):
    try:
        output = subprocess.check_output(cmd).decode().strip()
        if output == "All checks passed!":
            return True
        else:
            print(output)
            return False
    except subprocess.CalledProcessError as cpe:
        print("The command [%s] failed:\n%s" % (' '.join(cmd), cpe.output.decode('utf-8')))
        return False

cmd = ["ruff", "check", "--fix", "../.", "."]

if not run(cmd):
    sys.exit(1)
