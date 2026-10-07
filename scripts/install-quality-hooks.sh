#!/bin/sh
set -eu

python3 -m pip install -r requirements-dev.txt
python3 -m pre_commit install \
  --install-hooks \
  --hook-type pre-commit \
  --hook-type pre-push
