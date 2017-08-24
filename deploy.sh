#!/bin/bash

set -e
echo "cleaning dist/*"
rm -rf dist/*

echo "Generating the binary package...."
python ./setup.py sdist

echo "Uploading the package to PyPi"
twine upload dist/*
