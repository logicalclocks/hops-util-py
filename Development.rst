============
Development
============

Setting up development environment
---------------------------------------------------
This section shows a way to configure a development environment that allows you to run tests and build documentation.

The recommended way to run the unit tests is to use the Dockerized Linux workspace via the Makefile provided at :code:`docker/Makefile`.
The commands below will build the Docker image, start a running container with your local hops-util-py source code mounted
into it from the host at :code:`/hops`. A python 3.6 environment is available in the container at :code:`/hops_venv3.6/`.
Moreover, the commands below will also open a BASH shell into it (you must have GNU Make and Docker installed beforehand (:code:`sudo apt-get install docker.io`)):

.. code-block:: bash

    cd docker # go to where Makefile is located

    # Build docker container and mount (you might need sudo),
    # this will fail if you already have a container named "hops" running
    # (if that is the case, just run `make shell` (or `sudo make shell`) to login to the existing container instead,
    # or if you want the kill the existing container and re-build, you can execute: `make clean` and then make build run shell)
    make build run shell # this might require sudo

    # Run the unit tests
    cd /hops/docker # once inside docker
    ./run_tests.sh 3.6 # for python 3.6
    ./run_tests.sh 3.6 -s # run tests with verbose flag

    # Alternatively you can skip the bash scripts and write the commands yourself (this gives you more control):
    cd /hops #inside the container
    source /hops_venv3.6/bin/activate # for python 3.6
    pip install -e . # install latest source
    pytest -v hops # run tests, note if you want to re-run just edit the code in your host and run the same command, you do not have to re-run pip install..

The docker approach is recommended if you already have an existing SPARK/Hadoop installation in your environment to avoid conflicts
( for example if you want to run the tests from inside a VM where Hops is deployed). Moreover, using docker makes it simpler to test python 3.6.

Also note that when you edit files inside your local machine the changes will automatically get reflected in the docker
container since the directory is mounted there so you can easily re-run tests during development as you do code-changes.

To open up a shell in an already built :code:`hops` container:

.. code-block:: bash

    cd docker # go to where Makefile is located
    make shell


To kill an existing :code:`hops` container:


.. code-block:: bash

    cd docker # go to where Makefile is located
    make clean

If you want to run the tests in your local environment (watch out for spark/hadoop/environment conflicts) you can use the following setup:

.. code-block:: bash

    sudo apt-get install python3-dev  # install python3
    sudo apt-get install libsasl2-dev # for SSL used in kafka
    pip install virtualenv
    rm -rf env # remove old env if it exists
    virtualenv env --python <path-to-python3 or python2> #creates a virtual env for tests (e.g virtualenv env --python /usr/bin/python3.5)
    source env/bin/activate #activates the env
    pip install -U pip setuptools #install dependency
    pip install -e .[docs,tf,test] #install hops together with dependencies and extra dependencies for docs,tensorflow and tests
    pytest -v hops # run tests

Unit tests
----------
To run unit tests locally:

.. code-block:: bash

    pytest -v hops # Runs all tests
    pytest -v hops/tests/test_featurestore.py # Run specific test module
    pytest -v hops/tests/test_featurestore.py -k 'test_project_featurestore' # Run specific test in module
    pytest -m prepare # run test setups before parallel execution. **Note**: Feature store test suite is best run sequentially, otherwise race-conditions might cause errors.
    pytest -v hops -n 5 # Run tests in parallel with 5 workers. (Run prepare first)
    pytest -v hops -n auto #Run with automatically selected number of workers
    pytest -v hops -s # run with printouts (stdout)

Documentation
-------------

We use sphinx to automatically generate API-docs

.. code-block:: bash

    pip install -e .[docs]
    cd docs; make html

Integration Tests
-------------

The notebooks in :code:`it_tests/` are used for integration testing by running them as jobs on a Hopsworks installation.
The integration tests can be triggered from https://github.com/logicalclocks/hops-testing by using the following steps:

1. Open a PR in hops-testing and override the :code:`test_manifesto` with the cookbooks you want to test
2. In your PR, add the following attribute in your vagrantfiles to run the integration tests: :code:`test:hopsworks:it = true`,
   e.g in Vagrantfile-centos and Vagrantfile-ubuntu add:

.. code-block:: bash

    config.vm.provision :chef_solo do |chef|
        chef.cookbooks_path = "cookbooks"
        chef.json = {
          "test" => {
            "hopsworks" => {
	           "it" => true
            }
          }
        }

3. If you need to test a version of hops-util-py that is not merged you can set the chef attributes in your cluster-definition
   as follows to use a branch called :code:`test` in repository of :code:`kim/hops-util-py`:

.. code-block:: bash

    conda:
      hops-util-py:
        install-mode: "git"
        branch: "test"
        repo: "kim"
