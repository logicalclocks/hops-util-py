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
        'pathlib',
        'pyhopshive[thrift]',
        'confluent-kafka',
        'hops-petastorm',
        'opencv-python',
        'boto3',
        'pyopenssl',
        'idna',
        'cryptography',
        'pyarrow==0.14.1'
    ],
    extras_require={
        'pydoop': ['pydoop'],
        'tf': ['tensorflow'],
        'tf_gpu': ['tensorflow-gpu'],
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
        'plotting': ['matplotlib', 'seaborn'],
        'pynvm': ['nvidia-ml-py']
    },
    author='Robin Andersson',
    author_email='robin.eric.andersson@gmail.com',
    description='A helper library for Hops that facilitates development by hiding the complexity of discovering services and setting up security.',
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ]
)
