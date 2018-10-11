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
cd docs; sphinx-apidoc -f -o source/ ../hops ../hops/distribute/ ../hops/launcher.py ../hops/grid_search.py ../hops/differential_evolution.py ../hops/version.py; make html; cd ..

echo "Uploading the docs to snurran"
scp -r docs/build/html/* glassfish@snurran.sics.se:/var/www/hops/hops-util-py-docs/
