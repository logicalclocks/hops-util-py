pipeline {
  agent {
    node {
      label 'platform_testing'
    }
  }

  stages {
    stage ('setup') {
      steps {
        // Creates the virtualenv before proceeding
        sh """
        if [ ! -d $WORKSPACE/../hops-util-py-env ];
        then
          virtualenv --python=/usr/bin/python $WORKSPACE/../hops-util-py-env
          $WORKSPACE/../hops-util-py-env/bin/pip install twine sphinx sphinx-autobuild recommonmark sphinx_rtd_theme jupyter_sphinx_theme
        fi
        rm -rf dist/*
        """
      }
		}
    stage ('build') {
      steps {
        sh """
        source $WORKSPACE/../hops-util-py-env/bin/activate
        python ./setup.py sdist
        """
      }
    }
    stage ('build-doc') {
      steps {
        sh """
        source $WORKSPACE/../hops-util-py-env/bin/activate
        cd docs; sphinx-apidoc -f -o source/ ../hops ../hops/distribute/; make html; cd ..
        """
      }
    }
    stage ('deploy-doc') {
      steps {
        sh 'scp -r docs/build/html/* jenkins@hops-py.logicalclocks.com:/var/www/hops-py'
      }
    }
    stage ('deploy-bin') {
      environment {
        PYPI = credentials('977daeb0-e1c8-43a0-b35a-fc37bb9eee9b')
      }
      steps {
        sh """
        source $WORKSPACE/../hops-util-py-env/bin/activate
    	  twine upload -u $PYPI_USR -p $PYPI_PSW dist/*
        """
      }
    }
  }
}
