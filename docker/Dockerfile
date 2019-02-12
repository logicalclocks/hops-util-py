FROM ubuntu:16.04
# This container installs hops-util-py and necessary dependencies to run tests

# Install dependencies
RUN apt-get update -qq
RUN apt-get install -qq -y software-properties-common
RUN apt-get install -qq -y libsasl2-dev
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt-get update -qq
RUN apt-get -qq install -y \
  build-essential \
  default-jre \
  git \
  libglib2.0-0 \
  libsm6 \
  libxrender1 \
  python-pip \
  python-software-properties \
  python2.7 \
  python2.7-dev \
  python3-pip \
  python3.6 \
  python3.6-dev \
  python3.6-venv \
  virtualenv \
  && rm -rf /var/lib/apt/lists/*

# Temporarily add files needed for env setup.
RUN mkdir /hops
ADD setup.py /hops/
ADD README.rst /hops/
ADD hops /hops/hops

# Set up Python3 environment
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel
RUN python3.6 -m venv /hops_venv3.6
RUN /hops_venv3.6/bin/pip3.6 install -e /hops/[docs,tf,test] # install dependencies
RUN /hops_venv3.6/bin/pip3.6 uninstall -y hops # uninstall hops since we will use the latest source when running tests

# Set up Python2 environment
RUN virtualenv /hops_venv2.7
RUN python2.7 -m pip install pip --upgrade
RUN /hops_venv2.7/bin/pip2.7 install -e /hops/[docs,tf,test] # install dependencies
RUN /hops_venv2.7/bin/pip2.7 uninstall -y hops # uninstall hops since we will use the latest source when running tests

# Clean up temporary files and make room for mounted hops dir
RUN rm -r /hops
