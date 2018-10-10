pipeline {
  agent {
    node {
      label 'platform_testing'
    }
  }

  stages {
	  stage ('setup') {
			steps {
        withPythonEnv('hops-util-build') {
    			// Creates the virtualenv before proceeding
    			sh 'pip install twine sphinx sphinx-autobuild recommonmark sphinx_rtd_theme jupyter_sphinx_theme'
					sh 'rm -rf dist/*'
				}
      }
		}
    stage ('build') {
			steps {
        withPythonEnv('hops-util-build') {
    			sh 'python ./setup.py sdist'
				}
      }
    }
    stage ('deploy-bin') {
			steps {
        withPythonEnv('hops-util-build') {
    			sh 'twine upload dist/*'
				}
      }
    }
  }
}
