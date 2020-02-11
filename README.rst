============
hops-util-py
============

|Downloads| |PypiStatus| |PythonVersions|

`hops-util-py` is a helper library for Hops that facilitates development by hiding the complexity of running applications, discovering services and interacting with HopsFS.

It provides an Experiment API to run Python programs such as TensorFlow, Keras and PyTorch on a Hops Hadoop cluster. A TensorBoard will be started when an Experiment begins and the contents of the logdir saved in your Project.

An Experiment could be a single Python program, which we refer to as an *Experiment*. Grid search or genetic hyperparameter optimization such as differential evolution which runs several Experiments in parallel, which we refer to as *Parallel Experiment*. The library supports ParameterServerStrategy and CollectiveAllReduceStrategy, making multi-machine/multi-gpu training as simple as invoking a function for orchestration. This mode is referred to as *Distributed Training*.

Moreover it provides an easy-to-use API for defining TLS-secured Kafka producers and consumers on the Hopsworks platform as well as an API for interacting with the Hopsworks Feature Store

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


------------------------
Development Instructions
------------------------

For development details such as how to test and build docs, see this reference: Development_.

.. _Development: ./Development.rst

-------------
Documentation
-------------

An overview of HopsML, a python-first ML pipeline is available here: hopsML_

Example notebooks for doing deep learning and big data processing on Hops are available here: hops-examples_

API documentation is available here: API-docs_


.. _hops-examples: https://github.com/logicalclocks/hops-examples
.. _hopsML: https://hops.readthedocs.io/en/latest/hopsml/hopsML.html
.. _API-docs: http://hops-py.logicalclocks.com/



------------------------------------
Quick Start: Python with HopsML
------------------------------------

Hops uses PySpark to distribute the execution of Python programs in a cluster. PySpark applications consist of two main components, a Driver and one to many Executors. The Driver and the Executors can be started on potentially any host in the cluster and use both the network and the HDFS filesystem to coordinate.


Restructuring Python Programs as PySpark Programs
--------------------------------------------------------------------

If you want to run a Python program, e.g.,  to train a neural network on a GPU on Hops, you will need to restructure your code. The reason for this is that your single Python process needs to be restructured as a PySpark program, see the figure below.

.. _hopsml-pyspark.png: imgs/hopsml-pyspark.png
.. figure:: imgs/hopsml-pyspark.png
    :alt: HopsML Python Program
    :target: `hopsml-pyspark.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center

The good news is that all you will need to do to get started is to move your code inside a function. In the code snippet below, the Executor code is on lines 1-3 (the *train* function) and the Driver code is on lines 5-7. For the Executor, you define a function (e.g., *train*, but the function can have any name).  The code in the function will get run on Executors (containers). To invoke the Executor function (*train*) from the Driver (the main part of your Python program), you use the Experiment API. Launch a single Executor with *experiment.launch(<fn_name>)*.  Launch many Executors with *experiment.grid_search(<fn_name>)* for hyperparameter optimization, and *experiment.collective_all_reduce(<fn_name>)* for distributed training.


.. code-block:: python

  def train():
    import tensorflow as tf
    # training code here

  # Driver code starts here
  from hops import experiment
  experiment.launch(train)


.. _driver.png: imgs/driver.png
.. figure:: imgs/driver.png
    :alt: HopsML Python Program
    :target: `driver.png`_
    :align: center
    :scale: 90 %
    :figclass: align-center


Logging in the Driver
---------------------------
When you print to stdout and stderr in the Driver program, the output is printed in the Jupyter console.

.. code-block:: python

   # main scope of program or any non-Executor function
   print('log message is printed to Jupyter cell output')


Logging to stdout/stderr in the Executor
------------------------------------------------------

If you execute print(‘...’) in the executor, it will send the output to stdout and stderr on the executor. This will not be displayed in Jupyter console. You can, however, read output in the executors using the Spark UI. As soon as the Spark application has exited, these logs are cleaned up - they are no longer available.

.. code-block:: python

  train():
    # This will write to stdout/stderr on the Spark Executors
    # You can only view this log entry from the Spark UI while the application
    # is running.
    print("Executor log message - not visible in Jupyter, visible in Spark UI")


To access the Spark executor logs, you will need 4 clicks on your mouse:
1. Select the UI for the application you started running from Jupyter (click on the button inside the yellow highlighter in the image below):

.. _executor-stderr1.png: imgs/executor-stderr1.png
.. figure:: imgs/executor-stderr1.png
    :alt: Stdout-err-1
    :target: `executor-stderr1.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


2.  Select the “Executors” tab from the Spark UI (click on the button inside the yellow highlighter):

.. _executor-stderr2.png: imgs/executor-stderr2.png
.. figure:: imgs/executor-stderr2.png
    :alt: Stdout-err-2
    :target: `executor-stderr2.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


3. Now you should see all the Executors that are running (active) or have finished running more than 90 seconds ago (dead). There will be stdout and stderr logs available for every Executor here - if you ran with 10 GPUs, with 1 GPU per Executor, there will be 10 different stdout and 10 different stderr log files available.. Click on the stderr or stdout log for the Executor you want to examine (yellow highlighted text below):

.. _executor-stderr3.png: imgs/executor-stderr3.png
.. figure:: imgs/executor-stderr3.png
    :alt: Stdout-err-3
    :target: `executor-stderr3.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


4. Now you can see the logs for that Executor on the screen:

.. _executor-stderr4.png: imgs/executor-stderr4.png
.. figure:: imgs/executor-stderr4.png
    :alt: Stdout-err-4
    :target: `executor-stderr4.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center

Logging to file (HDFS) in the Executor or Driver
---------------------------------------------------

You can also write log messages from either an Executor or Driver to the same logfile in HDFS.

.. code-block:: python

  train():
    # This will write to your Experiments/ directory in your project
    from hops import hdfs
    hdfs.log("This is written to the logfile in the Experiments dataset, not output in Jupyter cell.")

You can navigate to the log file created in the Datasets view in Hopsworks for your project, inside the Experiments dataset. The file created will be called “logfile” and if you right-click on it, you can preview its contents to see the first or last 1000 lines in the file. If you have the data-owner role in the project, you will also be allowed to download this file from here.

.. _executor-hdfs-log.png: imgs/executor-hdfs-log.png
.. figure:: imgs/executor-hdfs-log.png
    :alt: hdfs-log
    :target: `executor-hdfs-log.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center

Note that the default log file is the same for all Executors. If many Executors write concurrently to the same file, this may have negative performance implications as Executors may block, waiting for write access to the file. In large-scale experiments, you can configure each Executors to write to its own log file (append a unique ID to the filename).



Installing Python Libraries in Hopsworks
---------------------------------------------

You can use the ‘Conda’ and ‘Pip’ services in Hopsworks to install python libraries. In the ‘Conda’ service, you can change the conda repository by double-clicking on it and entering the URL for a new repo (or ‘default’ for the standard conda repository).

Note: Pillow and matplotlib do not work from conda. Install using “pip”, instead.


Plotting with Sparkmagic in Jupyter
---------------------------------------------

Hopsworks supports both the Python kernel and Sparkmagic kernel. Plotting in the Python kernel is usually handled by libraries such as matplotlib and seaborne. These libraries can also be used in the Sparkmagic kernel, but require more work from the developer, as dataframes in Spark are distributed in the cluster and need to be localized to the Jupyter notebook server as Pandas dataframes, in order to be plotted.
When you run a PySpark program with the Sparkmagic kernel in Jupyter, you will not need to initialize a Spark context, as it is done automatically for you (by Sparkmagic). However, as the PySpark application is not running on the same host as the Jupyter notebook server, plotting (with matplotlib) will not work as normal in a Python kernel. The main change you need to make is to use ‘magics’ in the sparkmagic kernel to get Spark or Pandas dataframes to be localized to the Jupyter notebook server, from where they can be visualized. More details are found in the reference notebook below. Information on the magics available in Sparkmagic are found in the link below.


Adding Python modules to a Jupyter notebook
---------------------------------------------

.. _add-python-module.png: imgs/add-python-module.png
.. figure:: imgs/add-python-module.png
    :alt: add-python-module
    :target: `add-python-module.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


API for the Hopsworks Feature Store
--------------------------------------------------------------------
Hopsworks has a data management layer for machine learning, called a feature store.
The feature store enables simple and efficient versioning, sharing, governance and definition of features that can be used to both train machine learning models or to serve inference requests.
The featurestore serves as a natural interface between data engineering and data science.

**Writing to the featurestore**:

.. code-block:: python

  raw_data = spark.read.format("csv").load(filename)
  polynomial_features = raw_data.map(lambda x: x^2)
  from hops import featurestore
  featurestore.insert_into_featuregroup(polynomial_features, "polynomial_features")

**Reading from the featurestore**:

.. code-block:: python

  from hops import featurestore
  features_df = featurestore.get_features(["team_budget", "average_attendance", "average_player_age"])

**Integration with Sci-kit Learn**:

.. code-block:: python

  from hops import featurestore
  train_df = featurestore.get_featuregroup("iris_features", dataframe_type="pandas")
  x_df = train_df[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
  y_df = train_df[["label"]]
  X = x_df.values
  y = y_df.values.ravel()
  iris_knn = KNeighborsClassifier()
  iris_knn.fit(X, y)

**Integration with Tensorflow**:

.. code-block:: python

  from hops import featurestore
  features_df = featurestore.get_features(
      ["team_budget", "average_attendance", "average_player_age",
      "team_position", "sum_attendance",
       "average_player_rating", "average_player_worth", "sum_player_age",
       "sum_player_rating", "sum_player_worth", "sum_position",
       "average_position"
      ]
  )
  featurestore.create_training_dataset(features_df, "team_position_prediction", data_format="tfrecords")

  def create_tf_dataset():
      dataset_dir = featurestore.get_training_dataset_path("team_position_prediction")
      input_files = tf.gfile.Glob(dataset_dir + "/part-r-*")
      dataset = tf.data.TFRecordDataset(input_files)
      tf_record_schema = featurestore.get_training_dataset_tf_record_schema("team_position_prediction")
      feature_names = ["team_budget", "average_attendance", "average_player_age", "sum_attendance",
           "average_player_rating", "average_player_worth", "sum_player_age", "sum_player_rating", "sum_player_worth",
           "sum_position", "average_position"
          ]
      label_name = "team_position"

      def decode(example_proto):
          example = tf.parse_single_example(example_proto, tf_record_schema)
          x = []
          for feature_name in feature_names:
              x.append(example[feature_name])
          y = [tf.cast(example[label_name], tf.float32)]
          return x,y

      dataset = dataset.map(decode).shuffle(SHUFFLE_BUFFER_SIZE).batch(BATCH_SIZE).repeat(NUM_EPOCHS)
      return dataset
  tf_dataset = create_tf_dataset()

**Integration with PyTorch**:

.. code-block:: python

  from hops import featurestore
  df_train=...
  featurestore.create_training_dataset(df_train, "MNIST_train_petastorm", data_format="petastorm")

  from petastorm.pytorch import DataLoader
  train_dataset_path = featurestore.get_training_dataset_path("MNIST_train_petastorm")
  device = torch.device('cuda' if use_cuda else 'cpu')
  with DataLoader(make_reader(train_dataset_path, num_epochs=5, hdfs_driver='libhdfs', batch_size=64) as train_loader:
          model.train()
          for batch_idx, row in enumerate(train_loader):
              data, target = row['image'].to(device), row['digit'].to(device)

**Feature Visualizations**:

.. _feature_plots1.png: imgs/feature_plots1.png
.. figure:: imgs/feature_plots1.png
    :alt: Visualizing feature distributions
    :target: `feature_plots1.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center


.. _feature_plots2.png: imgs/feature_plots2.png
.. figure:: imgs/feature_plots2.png
    :alt: Visualizing feature correlations
    :target: `feature_plots2.png`_
    :align: center
    :scale: 75 %
    :figclass: align-center

Model Serving API
--------------------------------------------------------------------

In the `serving` module you can find an API for creating/starting/stopping/updating models being served on Hopsworks as well as making inference requests.

.. code-block:: python

  from hops import serving
  from hops import model

  # Tensorflow
  export_path = work_dir + '/model'
  builder = tf.saved_model.builder.SavedModelBuilder(export_path
  ... # tf specific export code
  model.export(export_path, "mnist")
  model_path="/Models/mnist/"
  SERVING_NAME="mnist"
  serving.create_or_update(model_path, "mnist", serving_type="TENSORFLOW", model_version=1)
  if serving.get_status("mnist") == 'Stopped':
      serving.start("mnist")
  data = {"signature_name": 'predict_images', "instances": [np.random.rand(784).tolist()]}
  response = serving.make_inference_request(SERVING_NAME, data)

   # SkLearn
  script_path = "Jupyter/Serving/sklearn/iris_flower_classifier.py"
  model.export(script_path, "irisClassifier")
  if serving.exists("irisClassifier"):
      serving.delete("irisClassifier")
  serving.create_or_update(script_path, "irisClassifier", serving_type="SKLEARN", model_version=1)
  serving.start("irisClassifier")
  data = {"inputs" : [[random.uniform(1, 8) for i in range(NUM_FEATURES)]]}
  response = serving.make_inference_request(SERVING_NAME, data)

Kafka API
--------------------------------------------------------------------

In the `kafka` module you can find an API to interact with kafka topics in Hopsworks.

.. code-block:: python

  from hops import kafka, serving
  from confluent_kafka import Producer, Consumer, KafkaError
  TOPIC_NAME = serving.get_kafka_topic(SERVING_NAME) # get inference logs
  config = kafka.get_kafka_default_config()
  config['default.topic.config'] = {'auto.offset.reset': 'earliest'}
  consumer = Consumer(config)
  topics = [TOPIC_NAME]
  consumer.subscribe(topics)
  json_schema = kafka.get_schema(TOPIC_NAME)
  avro_schema = kafka.convert_json_schema_to_avro(json_schema)
  msg = consumer.poll(timeout=1.0)
  value = msg.value()
  event_dict = kafka.parse_avro_msg(value, avro_schema)


HDFS API
--------------------------------------------------------------------

In the `hdfs` module you can find a high-level API for interacting with the distributed file system

.. code-block:: python

  from hops import hdfs
  hdfs.ls("Logs/")
  hdfs.cp("Resources/test.txt", "Logs/")
  hdfs.mkdir("Logs/test_dir")
  hdfs.rmr("Logs/test_dir")
  hdfs.move("Logs/README_dump_test.md", "Logs/README_dump_test2.md")
  hdfs.chmod("Logs/README.md", 700)
  hdfs.exists("Logs/")
  hdfs.copy_to_hdfs("test.txt", "Resources", overwrite=True)
  hdfs.copy_to_local("Resources/test.txt", overwrite=True)

Experiment API
--------------------------------------------------------------------

In the `experiment` module you can find an API for launching reproducible machine learning experiments.
Standalone experiments, distributed experiments, hyperparameter tuning and many more are supported.

.. code-block:: python

  from hops import experiment
  log_dir, best_params = experiment.differential_evolution(
      train_fn,
      search_dict,
      name='team_position_prediction_hyperparam_search',
      description='Evolutionary search through the search space of hyperparameters with parallel executors to find the best parameters',
      local_logdir=True,
      population=4,
      generations = 1
  )


References
--------------

- https://github.com/logicalclocks/hops-examples/blob/master/tensorflow/notebooks/Plotting/data_visualizations.ipynb
- https://github.com/jupyter-incubator/sparkmagic/blob/master/examples/Magics%20in%20IPython%20Kernel.ipynb

.. |Downloads| image:: https://pepy.tech/badge/hops
   :target: https://pepy.tech/project/hops
.. |PypiStatus| image:: https://img.shields.io/pypi/v/hops.svg
    :target: https://pypi.org/project/hops
.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/hops.svg
    :target: https://travis-ci.org/hops
