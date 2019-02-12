#!/usr/bin/env bash

PYTHON_VER=$1

source /hops_venv${PYTHON_VER}/bin/activate
cd /hops
pip install -e .
pytest -v hops

