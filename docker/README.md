# Dockerfile

This docker file is used to create a reproducible environment for running `hops-util-py` Unit tests for python3.6 and python 2.7.

To build the docker image:
  
```bash
cd docker # go to where Makefile is located
# Build docker container and mount (you might need sudo),
# this will fail if you already have a container named "hops" running
# (if that is the case, just run `make shell` instead, or if you want the kill the existing container and re-build,
# execute: `make clean`)
make build run shell
```
The docker approach is recommended if you already have an existing SPARK/Hadoop installation in your environment to avoid conflicts ( for example if you want to run the tests from inside a VM where Hops is deployed).

Also note that when you edit files inside your local machine the changes will automatically get reflected in the docker container as well since the directory is mounted there.

To open up a shell in an already built container:

```bash
cd docker # go to where Makefile is located
make shell
```

To kill an existing container:

```bash
cd docker # go to where Makefile is located
make clean
```