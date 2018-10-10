============
hops-util-py
============

`hops-utily-py` is a helper library for Hops that facilitates development by hiding the complexity of discovering services and setting up security for python programs interacting with Hops services.

It provides an API to scale out TensorFlow training on a Hops Hadoop cluster. Providing first-class support to Hyper-parameter search, Distributed TensorFlow and Horovod.

Moreover it provides an easy-to-use API for defining TLS-secured Kafka producers and consumers on the Hops platform.

-----------
Quick Start
-----------

To Install:

>>> pip install hops

Sample usage:

>>> from hops import experiment
>>> from hops import hdfs
>>> notebook = hdfs.project_path() + "Jupyter/Experiment/..." #path to your notebook
>>> experiment.launch(minimal_mnist, #minimal_mnist is your training function
>>>                   name='mnist estimator',
>>>                   description='A minimal mnist example with two hidden layers',
>>>                   versioned_resources=[notebook]

To build docs:

>>> cd docs
>>> sphinx-apidoc -f -o source/ ../hops ../hops/distribute/
>>> make html


-------------
Documentation
-------------

Tutorials and general documentation is available here: hops-examples_

Example notebooks for doing deep learning and big data processing on Hops is available here: hops-io_

API documentation is available here: TODO


.. _hops-examples: https://github.com/logicalclocks/hops-examples
.. _hops-io: https://hops.readthedocs.io/en/latest/user_guide/tensorflow/hops.html