============
hops-util-py
============

`hops-util-py` is a helper library for Hops that facilitates development by hiding the complexity of running applications, discovering services and interacting with HopsFS.

It provides an Experiment API to run Python programs such as TensorFlow, Keras and PyTorch on a Hops Hadoop cluster. A TensorBoard will be started when an Experiment begins and the contents of the logdir saved in your Project. An Experiment could be a single Python program, which we refer to as an *Experiment*.

Grid search or genetic hyperparameter optimization such as differential evolution which runs several Experiments in parallel, which we refer to as *Parallel Experiment*.

The library supports ParameterServerStrategy and CollectiveAllReduceStrategy, making multi-machine/multi-gpu training as simple as invoking a function for orchestration. This mode is referred to as *Distributed Training*.

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
>>> # minimal_mnist is a function you defined
>>> experiment.launch(minimal_mnist, #minimal_mnist is your training function
>>>                   name='mnist estimator',
>>>                   description='A minimal mnist example with two hidden layers',
>>>                   versioned_resources=[notebook]

To build docs:

>>> cd docs; sphinx-apidoc -f -o source/ ../hops ../hops/distribute/ ../hops/launcher.py ../hops/grid_search.py ../hops/differential_evolution.py ../hops/version.py ../hops/constants.py; make html; cd ..


-------------
Documentation
-------------

Tutorials and general documentation is available here: hops-examples_

Example notebooks for doing deep learning and big data processing on Hops is available here: hops-io_

API documentation is available here: API-docs_


.. _hops-examples: https://github.com/logicalclocks/hops-examples
.. _hops-io: https://hops.readthedocs.io/en/latest/user_guide/tensorflow/hops.html
.. _API-docs: http://hops-py.logicalclocks.com/
