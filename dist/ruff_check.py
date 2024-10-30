#!/usr/bin/env python3

from shutil import which
import pathlib
import re
import subprocess
import sys

# Always use this script's folder as the working directory.
CURRENT_DIR = pathlib.Path(__file__).parent.resolve()


def check_ruff_version(expected_version: str) -> bool:
    cmd = ["ruff", "--version"]
    try:
        output = subprocess.check_output(cmd, cwd=CURRENT_DIR).decode().strip()
    except Exception as e:
        print(e)
        return False

    if not output == f"ruff {expected_version}":
        print(
            f"Unexpected Ruff version detected: {output} instead of {expected_version}"
        )
        return False
    return True


def get_expected_ruff_version(config_file: str) -> str:
    ruff_version = None
    with open(config_file) as file:
        for line in file:
            m = re.search(r'required-version = "==(\d.\d.\d)"', line.strip())
            if m:
                ruff_version = m.group(1)
                break
    assert ruff_version, f"No ruff version has been found in {config_file}"
    return ruff_version


def run_ruff_checks(cmd: list[str]) -> bool:
    try:
        output = subprocess.check_output(cmd, cwd=CURRENT_DIR).decode().strip()
        if output == "All checks passed!":
            return True
        else:
            print(output)
            return False
    except subprocess.CalledProcessError as cpe:
        print(
            "The command [%s] failed:\n%s" % (" ".join(cmd), cpe.output.decode("utf-8"))
        )
        return False


def show_ruff_installation_steps(expected_version: str) -> None:
    doc_link = "https://docs.astral.sh/ruff/installation/"
    print(f"Please install Ruff using the official documentation: {doc_link}\n")
    print("Suggested steps using a virtual environment:")
    print(f"virtualenv -p python3 {CURRENT_DIR.parent}/venv")
    print(f"source {CURRENT_DIR.parent}/venv/bin/activate")
    print(f"python3 -m pip install ruff=={expected_version}\n")


def main():
    ruff_toml_file = f"{CURRENT_DIR}/ruff.toml"
    ruff_expected_version = get_expected_ruff_version(ruff_toml_file)

    # Check the correct version of Ruff is installed.
    if not which("ruff") or not check_ruff_version(ruff_expected_version):
        print("Please check the right version of Ruff is installed.")
        show_ruff_installation_steps(ruff_expected_version)
        sys.exit(1)

    cmd = ["ruff", "check", "--fix", "../.", ".", "--config", ruff_toml_file]

    if not run_ruff_checks(cmd):
        sys.exit(1)


if __name__ == "__main__":
    main()
