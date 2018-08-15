import os
from setuptools import setup, find_packages

exec(open('hops/version.py').read())

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='hops',
    version=__version__,
    author='Robin Andersson',
    author_email='robin.eric.andersson@gmail.com',
    description='A helper library for Hops that facilitates development by hiding the complexity of discovering services and setting up security.',
    license='Apache License 2.0',
    keywords='Hops, Hadoop, TensorFlow, Spark',
    url='https://github.com/logicalclocks/hops-util-py',
    download_url = 'http://snurran.sics.se/hops/hops-util-py/hops-' + __version__ + '.tar.gz',
    packages=find_packages(),
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    install_requires=['hopsfacets']
)
