import os
from setuptools import setup, find_packages

exec(open('hops/version.py').read())

def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname), encoding='utf8').read() #python3
    except:
        return open(os.path.join(os.path.dirname(__file__), fname)).read() #python2

setup(
    name='hops',
    version=__version__,
    install_requires=[
        'numpy',
        'pandas',
        'pyjks',
        'pyhopshive[thrift]',
        'boto3',
        'pyopenssl',
        'idna',
        'cryptography',
        'dnspython==1.16.0',
        'nvidia-ml-py3==7.352.0',
        'requests'
    ],
    extras_require={
        'pydoop': ['pydoop'],
        'tf': ['tensorflow'],
        'docs': [
            'sphinx',
            'sphinx-autobuild',
            'recommonmark',
            'sphinx_rtd_theme',
            'jupyter_sphinx_theme'
        ],
        'test': [
            'mock',
            'pytest',
        ],
        'spark': ['pyspark==2.4.3'],
        'plotting': ['matplotlib', 'seaborn']
    },
    author='Robin Andersson',
    author_email='robin.eric.andersson@gmail.com',
    description='Client library for interacting with Hopsworks, a full-stack platform for scale-out data science.',
    license='Apache License 2.0',
    keywords='Hops, Hadoop, TensorFlow, Spark',
    url='https://github.com/logicalclocks/hops-util-py',
    download_url='http://snurran.sics.se/hops/hops-util-py/hops-' + __version__ + '.tar.gz',
    packages=find_packages(exclude=['tests']),
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ]
)
