"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

from hops import hdfs as hopshdfs

from hops import differential_evolution as diff_evo
from hops import grid_search as gs
from hops import launcher as launcher
from hops import allreduce as allreduce
from hops.tensorflowonspark import TFCluster
from hops import tensorboard

from hops import util

from datetime import datetime
import atexit
import json
import pydoop.hdfs
import os
import subprocess

elastic_id = 1
app_id = None
experiment_json = None
running = False
driver_tensorboard_hdfs_path = None
run_id = 0

def get_logdir(app_id):
    global run_id
    return hopshdfs.get_experiments_dir() + '/' + app_id + '/begin/run.' +  str(run_id)

def begin(spark, name='no-name', local_logdir=False, versioned_resources=None, description=None):
    """ Start an experiment

    Args:
      :spark_session: SparkSession object
      :name: (optional) name of the job
    """
    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.stop() to stop it.")

    try:
        global app_id
        global experiment_json
        global elastic_id
        global run_id
        global driver_tensorboard_hdfs_path

        running = True

        sc = spark.sparkContext
        app_id = str(sc.applicationId)

        run_id = run_id + 1

        versioned_path = util.version_resources(versioned_resources, get_logdir(app_id))

        experiment_json = None

        experiment_json = util.populate_experiment(sc, name, 'experiment', 'begin', get_logdir(app_id), None, versioned_path, description)

        util.version_resources(versioned_resources, get_logdir(app_id))

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        hdfs_exec_logdir, hdfs_appid_logdir = hopshdfs.create_directories(app_id, run_id, None, 'begin')

        pydoop.hdfs.dump('', os.environ['EXEC_LOGFILE'], user=hopshdfs.project_user())

        hopshdfs.init_logger()

        driver_tensorboard_hdfs_path,_ = tensorboard.register(hdfs_exec_logdir, hdfs_appid_logdir, 0, local_logdir=local_logdir, tensorboard_driver=True)
    except:
        exception_handler()
        raise

    return

def end(metric=None):
    global running
    global experiment_json
    global elastic_id
    global driver_tensorboard_hdfs_path
    global app_id
    if not running:
        raise RuntimeError("An experiment is not running. Did you forget to call experiment.end()?")
    try:
        if metric:
            experiment_json = util.finalize_experiment(experiment_json, None, str(metric))
            util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)
        else:
            experiment_json = util.finalize_experiment(experiment_json, None, None)
            util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)
    except:
        exception_handler()
        raise
    finally:
        elastic_id +=1
        running = False
        handle = hopshdfs.get()

        if tensorboard.tb_pid != 0:
            subprocess.Popen(["kill", str(tensorboard.tb_pid)])

        if tensorboard.local_logdir_bool:
            local_tb = tensorboard.local_logdir_path
            util.store_local_tensorboard(local_tb, tensorboard.events_logdir)

        if not tensorboard.endpoint == None and not tensorboard.endpoint == '' \
                and handle.exists(tensorboard.endpoint):
            handle.delete(tensorboard.endpoint)
        hopshdfs.kill_logger()


def launch(spark, map_fun, args_dict=None, name='no-name', local_logdir=False, versioned_resources=None, description=None):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
      :spark_session: SparkSession object
      :map_fun: The TensorFlow function to run
      :args_dict: (optional) A dictionary containing hyperparameter values to insert as arguments for each TensorFlow job
      :name: (optional) name of the job
    """
    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.end() to stop it.")

    try:
        global app_id
        global experiment_json
        global elastic_id
        running = True

        sc = spark.sparkContext
        app_id = str(sc.applicationId)

        launcher.run_id = launcher.run_id + 1

        versioned_path = util.version_resources(versioned_resources, launcher.get_logdir(app_id))

        experiment_json = None
        if args_dict:
            experiment_json = util.populate_experiment(sc, name, 'experiment', 'launcher', launcher.get_logdir(app_id), json.dumps(args_dict), versioned_path, description)
        else:
            experiment_json = util.populate_experiment(sc, name, 'experiment', 'launcher', launcher.get_logdir(app_id), None, versioned_path, description)

        util.version_resources(versioned_resources, launcher.get_logdir(app_id))

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        retval, tensorboard_logdir = launcher.launch(sc, map_fun, args_dict, local_logdir)

        if retval:
            experiment_json = util.finalize_experiment(experiment_json, None, retval)
            util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)
            return tensorboard_logdir

        experiment_json = util.finalize_experiment(experiment_json, None, None)

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

    except:
        exception_handler()
        raise
    finally:
        elastic_id +=1
        running = False

    return tensorboard_logdir


def evolutionary_search(spark, objective_function, search_dict, direction = 'max', generations=10, population=10, mutation=0.5, crossover=0.7, cleanup_generations=False, name='no-name', local_logdir=False, versioned_resources=None, description=None):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
      :spark_session: SparkSession object
      :map_fun: The TensorFlow function to run
      :search_dict: (optional) A dictionary containing differential evolutionary boundaries
    """
    spark.sparkContext.setJobGroup("{}".format(name), "evolutionary search {}".format(name))
    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.end() to stop it.")

    try:
        global app_id
        global experiment_json
        global elastic_id
        running = True

        sc = spark.sparkContext
        app_id = str(sc.applicationId)

        diff_evo.run_id = diff_evo.run_id + 1

        versioned_path = util.version_resources(versioned_resources, diff_evo.get_logdir(app_id))

        experiment_json = None
        experiment_json = util.populate_experiment(sc, name, 'experiment', 'evolutionary_search', diff_evo.get_logdir(app_id), json.dumps(search_dict), versioned_path, description)

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        tensorboard_logdir, best_param, best_metric = diff_evo._search(spark, objective_function, search_dict, direction=direction, generations=generations, popsize=population, mutation=mutation, crossover=crossover, cleanup_generations=cleanup_generations, local_logdir=local_logdir)

        experiment_json = util.finalize_experiment(experiment_json, best_param, best_metric)

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        best_param_dict = util.convert_to_dict(best_param)

    except:
        exception_handler()
        raise
    finally:
        elastic_id +=1
        running = False

    return tensorboard_logdir, best_param_dict

def grid_search(spark, map_fun, args_dict, direction='max', name='no-name', local_logdir=False, versioned_resources=None, description=None):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
      :spark_session: SparkSession object
      :map_fun: The TensorFlow function to run
      :args_dict: A dictionary containing hyperparameter values to insert as arguments for each TensorFlow job
      :direction: 'max' to maximize, 'min' to minimize
      :name: (optional) name of the job
    """
    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.end() to stop it.")

    try:
        global app_id
        global experiment_json
        global elastic_id
        running = True

        sc = spark.sparkContext
        app_id = str(sc.applicationId)

        gs.run_id = gs.run_id + 1

        versioned_path = util.version_resources(versioned_resources, gs.get_logdir(app_id))

        experiment_json = util.populate_experiment(sc, name, 'experiment', 'grid_search', gs.get_logdir(app_id), json.dumps(args_dict), versioned_path, description)

        util.version_resources(versioned_resources, gs.get_logdir(app_id))

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        grid_params = util.grid_params(args_dict)

        tensorboard_logdir, param, metric = gs._grid_launch(sc, map_fun, grid_params, direction=direction, local_logdir=local_logdir)

        experiment_json = util.finalize_experiment(experiment_json, param, metric)

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)
    except:
        exception_handler()
        raise
    finally:
        elastic_id +=1
        running = False

    return tensorboard_logdir

def horovod(spark, notebook, name='no-name', local_logdir=False, versioned_resources=None, description=None):
    """ Run the notebooks specified in the path as input to horovod

    Args:
      :spark_session: SparkSession object
      :notebook: Notebook path
      :name: (optional) name of the job
    """
    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.end() to stop it.")

    try:
        global app_id
        global experiment_json
        global elastic_id
        running = True

        sc = spark.sparkContext
        app_id = str(sc.applicationId)

        allreduce.run_id = allreduce.run_id + 1

        versioned_path = util.version_resources(versioned_resources, allreduce.get_logdir(app_id))

        experiment_json = None
        experiment_json = util.populate_experiment(sc, name, 'experiment', 'horovod', allreduce.get_logdir(app_id), None, versioned_path, description)

        util.version_resources(versioned_resources, allreduce.get_logdir(app_id))

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

        tensorboard_logdir = allreduce.launch(sc, notebook, local_logdir=local_logdir)

        experiment_json = util.finalize_experiment(experiment_json, None, None)

        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

    except:
        exception_handler()
        raise
    finally:
        elastic_id +=1
        running = False

    return tensorboard_logdir

def exception_handler():
    global running
    global experiment_json
    if running and experiment_json != None:
        experiment_json = json.loads(experiment_json)
        experiment_json['status'] = "FAILED"
        experiment_json['finished'] = datetime.now().isoformat()
        experiment_json = json.dumps(experiment_json)
        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

def exit_handler():
    global running
    global experiment_json
    if running and experiment_json != None:
        experiment_json = json.loads(experiment_json)
        experiment_json['status'] = "KILLED"
        experiment_json['finished'] = datetime.now().isoformat()
        experiment_json = json.dumps(experiment_json)
        util.put_elastic(hopshdfs.project_name(), app_id, elastic_id, experiment_json)

atexit.register(exit_handler)
