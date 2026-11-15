#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PYTHONPYCACHEPREFIX=/tmp/profilr-pycache python3 -m profilr.cli demo
