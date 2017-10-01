import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='hopsutil',
    version='0.7.0',
    author='Robin Andersson',
    author_email='robin2@kth.se',
    description='A helper library for Hops that facilitates development by hiding the complexity of discovering services and setting up security.',
    license='Apache License 2.0',
    keywords='HOPS, Hadoop',
    url='https://github.com/hopshadoop/hops-util-python',
    download_url = 'https://github.com/hopshadoop/hops-util-python/archive/0.7.0.tar.gz',
    packages=['hopsutil'],
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    install_requires=[]
)
