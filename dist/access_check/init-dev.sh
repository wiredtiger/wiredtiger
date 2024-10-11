#!/bin/bash

set -ueo pipefail

[[ -f .venv/bin/activate ]] || virtualenv -p python3 .venv
chmod 755 .venv/bin/activate

. .venv/bin/activate
pip3 install -r requirements.txt

# set up development environment
pip3 install mypy types-regex

