#!/bin/bash

set -e
echo "cleaning dist/*"
rm -rf dist/*

echo "Generating the binary package...."
python ./setup.py sdist

echo "Uploading the package to PyPi"
twine upload dist/*

echo "Uploading the package to snurran"
scp dist/* glassfish@snurran.sics.se:/var/www/hops/hops-util-py/

echo "Building the docs"
cd docs; sphinx-apidoc -f -o source/ ../hops ../hops/distribute/; make html; cd ..

echo "Uploading the docs to snurran"
scp docs/build/html/* glassfish@snurran.sics.se:/var/www/hops/hops-util-py-docs/
