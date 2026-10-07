#!/bin/sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
quality_venv="$repo_root/.venv-quality"
python3 -m venv "$quality_venv"
"$quality_venv/bin/python" -m pip install -r "$repo_root/requirements-dev.txt"
git -C "$repo_root" config --local core.hooksPath .githooks
