"""
Experiment module used for running Experiments, Parallel Experiments (hyperparameter optimization) and Distributed Training on Hopsworks.

The programming model is that you wrap the code to run inside a training function.
Inside that training function provide all imports and parts that make up your experiment, see examples below.
Whenever a function to run an experiment is invoked it is also registered in the Experiments service.

*Three different types of experiments*
    - Run a single standalone Experiment using the *launch* function.
    - Run Parallel Experiments performing hyperparameter optimization using *grid_search* or *differential_evolution*.
    - Run single or multi-machine Distributed Training using *parameter_server* or *mirrored*.

"""

class Direction:
    MAX = "MAX"
    MIN = "MIN"

from hops.experiment_impl import launcher as launcher
from hops.experiment_impl.parallel import differential_evolution as diff_evo_impl, grid_search as grid_search_impl, \
    random_search as r_search_impl
from hops.experiment_impl.util import experiment_utils
from hops.experiment_impl.distribute import parameter_server as ps_impl, mirrored as mirrored_impl
from hops import util

import logging
log = logging.getLogger(__name__)

import time
import atexit
import json

run_id = 1
app_id = None
experiment_json = None
running = False

def launch(train_fn, args_dict=None, name='no-name', local_logdir=False, description=None, metric_key=None):
    """

    *Experiment* or *Parallel Experiment*

    Run an Experiment contained in *train_fn* one time with no arguments or multiple times with different arguments if
    *args_dict* is specified.

    Example usage:

    >>> from hops import experiment
    >>> def train_nn():
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    accuracy, loss = network.evaluate(learning_rate, layers, dropout)
    >>> experiment.launch(train_nn)

    Returning multiple outputs, including images and logs:

    >>> from hops import experiment
    >>> def train_nn():
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from PIL import Image
    >>>    f = open('logfile.txt', 'w')
    >>>    f.write('Starting training...')
    >>>    accuracy, loss = network.evaluate(learning_rate, layers, dropout)
    >>>    img = Image.new(.....)
    >>>    img.save('diagram.png')
    >>>    return {'accuracy': accuracy, 'loss': loss, 'logfile': 'logfile.txt', 'diagram': 'diagram.png'}
    >>> experiment.launch(train_nn)

    Args:
        :train_fn: The function to run
        :args_dict: If specified will run the same function multiple times with different arguments, {'a':[1,2], 'b':[5,3]} would run the function two times with arguments (1,5) and (2,3) provided that the function signature contains two arguments like *def func(a,b):*
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: A longer description for the experiment
        :metric_key: If returning a dict with multiple return values, this key should match the name of the key in the dict for the metric you want to associate with the experiment

    Returns:
        HDFS path in your project where the experiment is stored

    """

    num_ps = util.num_param_servers()
    assert num_ps == 0, "number of parameter servers should be 0"

    global running
    if running:
        raise RuntimeError("An experiment is currently running. Please call experiment.end() to stop it.")

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = None
        if args_dict:
            experiment_json = experiment_utils._populate_experiment(name, 'launch', 'EXPERIMENT', json.dumps(args_dict), description, app_id, None, None)
        else:
            experiment_json = experiment_utils._populate_experiment(name, 'launch', 'EXPERIMENT', None, description, app_id, None, None)

        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        logdir, return_dict = launcher._run(sc, train_fn, run_id, args_dict, local_logdir)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        metric = experiment_utils._get_metric(return_dict, metric_key)

        experiment_utils._finalize_experiment(experiment_json, metric, app_id, run_id, 'FINISHED', duration, logdir, None, None)
        return logdir, return_dict
    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def random_search(train_fn, boundary_dict, direction=Direction.MAX, samples=10, name='no-name', local_logdir=False, description=None, optimization_key='metric'):
    """

    *Parallel Experiment*

    Run an Experiment contained in *train_fn* for configured number of random samples controlled by the *samples* parameter. Each hyperparameter is contained in *boundary_dict* with the key
    corresponding to the name of the hyperparameter and a list containing two elements defining the lower and upper bound.
    The experiment must return a metric corresponding to how 'good' the given hyperparameter combination is.

    Example usage:

    >>> from hops import experiment
    >>> boundary_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    return network.evaluate(learning_rate, layers, dropout)
    >>> experiment.differential_evolution(train_nn, boundary_dict, direction='max')

    Returning multiple outputs, including images and logs:

    >>> from hops import experiment
    >>> boundary_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from PIL import Image
    >>>    f = open('logfile.txt', 'w')
    >>>    f.write('Starting training...')
    >>>    accuracy, loss = network.evaluate(learning_rate, layers, dropout)
    >>>    img = Image.new(.....)
    >>>    img.save('diagram.png')
    >>>    return {'accuracy': accuracy, 'loss': loss, 'logfile': 'logfile.txt', 'diagram': 'diagram.png'}
    >>> # Important! Remember: optimization_key must be set when returning multiple outputs
    >>> experiment.differential_evolution(train_nn, boundary_dict, direction='max', optimization_key='accuracy')


    Args:
        :train_fn: The function to run
        :boundary_dict: dict containing hyperparameter name and corresponding boundaries, each experiment randomize a value in the boundary range.
        :direction: Direction.MAX to maximize the returned metric, Direction.MIN to minize the returned metric
        :samples: the number of random samples to evaluate for each hyperparameter given the boundaries, for example samples=3 would result in 3 hyperparameter combinations in total to evaluate
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: A longer description for the experiment
        :optimization_key: When returning a dict, the key name of the metric to maximize or minimize in the dict should be set as this value

    Returns:
        HDFS path in your project where the experiment is stored, dict with best hyperparameters and return dict with best metrics

    """

    num_ps = util.num_param_servers()
    assert num_ps == 0, "number of parameter servers should be 0"

    global running
    if running:
        raise RuntimeError("An experiment is currently running.")

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = experiment_utils._populate_experiment(name, 'random_search', 'PARALLEL_EXPERIMENTS', json.dumps(boundary_dict), description, app_id, direction, optimization_key)
        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        logdir, best_param, best_metric, return_dict = r_search_impl._run(sc, train_fn, run_id, boundary_dict, samples, direction=direction, local_logdir=local_logdir, optimization_key=optimization_key)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        experiment_utils._finalize_experiment(experiment_json, best_metric, app_id, run_id, 'FINISHED', duration, experiment_utils._get_logdir(app_id, run_id), logdir, optimization_key)

        return logdir, best_param, return_dict
    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def differential_evolution(train_fn, boundary_dict, direction = Direction.MAX, generations=4, population=6, mutation=0.5, crossover=0.7, name='no-name', local_logdir=False, description=None, optimization_key='metric'):
    """
    *Parallel Experiment*

    Run differential evolution to explore a given search space for each hyperparameter and figure out the best hyperparameter combination.
    The function is treated as a blackbox that returns a metric for some given hyperparameter combination.
    The returned metric is used to evaluate how 'good' the hyperparameter combination was.

    Example usage:

    >>> from hops import experiment
    >>> boundary_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    import tensorflow
    >>>    return network.evaluate(learning_rate, layers, dropout)
    >>> experiment.differential_evolution(train_nn, boundary_dict, direction=Direction.MAX)

    Returning multiple outputs, including images and logs:

    >>> from hops import experiment
    >>> boundary_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from PIL import Image
    >>>    f = open('logfile.txt', 'w')
    >>>    f.write('Starting training...')
    >>>    accuracy, loss = network.evaluate(learning_rate, layers, dropout)
    >>>    img = Image.new(.....)
    >>>    img.save('diagram.png')
    >>>    return {'accuracy': accuracy, 'loss': loss, 'logfile': 'logfile.txt', 'diagram': 'diagram.png'}
    >>> # Important! Remember: optimization_key must be set when returning multiple outputs
    >>> experiment.differential_evolution(train_nn, boundary_dict, direction=Direction.MAX, optimization_key='accuracy')

    Args:
        :train_fn: the function to run, must return a metric
        :boundary_dict: a dict where each key corresponds to an argument of *train_fn* and the correspond value should be a list of two elements. The first element being the lower bound for the parameter and the the second element the upper bound.
        :direction: Direction.MAX to maximize the returned metric, Direction.MIN to minize the returned metric
        :generations: number of generations
        :population: size of population
        :mutation: mutation rate to explore more different hyperparameters
        :crossover: how fast to adapt the population to the best in each generation
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: a longer description for the experiment
        :optimization_key: When returning a dict, the key name of the metric to maximize or minimize in the dict should be set as this value

    Returns:
        HDFS path in your project where the experiment is stored, dict with best hyperparameters and return dict with best metrics

    """

    num_ps = util.num_param_servers()
    assert num_ps == 0, "number of parameter servers should be 0"

    global running
    if running:
        raise RuntimeError("An experiment is currently running.")

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        diff_evo_impl.run_id = run_id

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = experiment_utils._populate_experiment(name, 'differential_evolution', 'PARALLEL_EXPERIMENTS', json.dumps(boundary_dict), description, app_id, direction, optimization_key)
        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        logdir, best_param, best_metric, return_dict = diff_evo_impl._run(train_fn, boundary_dict, direction=direction, generations=generations, population=population, mutation=mutation, crossover=crossover, cleanup_generations=False, local_logdir=local_logdir, name=name, optimization_key=optimization_key)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        experiment_utils._finalize_experiment(experiment_json, best_metric, app_id, run_id, 'FINISHED', duration, experiment_utils._get_logdir(app_id, run_id), logdir, optimization_key)

        return logdir, best_param, return_dict

    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def grid_search(train_fn, grid_dict, direction=Direction.MAX, name='no-name', local_logdir=False, description=None, optimization_key='metric'):
    """
    *Parallel Experiment*

    Run grid search evolution to explore a predefined set of hyperparameter combinations.
    The function is treated as a blackbox that returns a metric for some given hyperparameter combination.
    The returned metric is used to evaluate how 'good' the hyperparameter combination was.

    Example usage:

    >>> from hops import experiment
    >>> grid_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    return network.evaluate(learning_rate, layers, dropout)
    >>> experiment.grid_search(train_nn, grid_dict, direction=Direction.MAX)

    Returning multiple outputs, including images and logs:

    >>> from hops import experiment
    >>> grid_dict = {'learning_rate': [0.1, 0.3], 'layers': [2, 9], 'dropout': [0.1,0.9]}
    >>> def train_nn(learning_rate, layers, dropout):
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from PIL import Image
    >>>    f = open('logfile.txt', 'w')
    >>>    f.write('Starting training...')
    >>>    accuracy, loss = network.evaluate(learning_rate, layers, dropout)
    >>>    img = Image.new(.....)
    >>>    img.save('diagram.png')
    >>>    return {'accuracy': accuracy, 'loss': loss, 'logfile': 'logfile.txt', 'diagram': 'diagram.png'}
    >>> # Important! Remember: optimization_key must be set when returning multiple outputs
    >>> experiment.grid_search(train_nn, grid_dict, direction=Direction.MAX, optimization_key='accuracy')

    Args:
        :train_fn: the function to run, must return a metric
        :grid_dict: a dict with a key for each argument with a corresponding value being a list containing the hyperparameters to test, internally all possible combinations will be generated and run as separate Experiments
        :direction: Direction.MAX to maximize the returned metric, Direction.MIN to minize the returned metric
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: a longer description for the experiment
        :optimization_key: When returning a dict, the key name of the metric to maximize or minimize in the dict should be set as this value

    Returns:
        HDFS path in your project where the experiment is stored, dict with best hyperparameters and return dict with best metrics

    """

    num_ps = util.num_param_servers()
    assert num_ps == 0, "number of parameter servers should be 0"

    global running
    if running:
        raise RuntimeError("An experiment is currently running.")

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = experiment_utils._populate_experiment(name, 'grid_search', 'PARALLEL_EXPERIMENTS', json.dumps(grid_dict), description, app_id, direction, optimization_key)
        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        grid_params = experiment_utils.grid_params(grid_dict)

        logdir, best_param, best_metric, return_dict = grid_search_impl._run(sc, train_fn, run_id, grid_params, direction=direction, local_logdir=local_logdir, name=name, optimization_key=optimization_key)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        experiment_utils._finalize_experiment(experiment_json, best_metric, app_id, run_id, 'FINISHED', duration, experiment_utils._get_logdir(app_id, run_id), logdir, optimization_key)

        return logdir, best_param, return_dict
    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def parameter_server(train_fn, name='no-name', local_logdir=False, description=None, evaluator=False, metric_key=None):
    """
    *Distributed Training*

    Sets up the cluster to run ParameterServerStrategy.

    TF_CONFIG is exported in the background and does not need to be set by the user themselves.

    Example usage:

    >>> from hops import experiment
    >>> def distributed_training():
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from hops import tensorboard
    >>>    from hops import devices
    >>>    logdir = tensorboard.logdir()
    >>>    ...ParameterServerStrategy(num_gpus_per_worker=devices.get_num_gpus())...
    >>> experiment.parameter_server(distributed_training, local_logdir=True)

    Args:f
        :train_fn: contains the code where you are using ParameterServerStrategy.
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: a longer description for the experiment
        :evaluator: whether to run one of the workers as an evaluator
        :metric_key: If returning a dict with multiple return values, this key should match the name of the key in the dict for the metric you want to associate with the experiment

    Returns:
        HDFS path in your project where the experiment is stored and return value from the process running as chief

    """
    num_ps = util.num_param_servers()
    num_executors = util.num_executors()

    assert num_ps > 0, "number of parameter servers should be greater than 0"
    assert num_ps < num_executors, "num_ps cannot be greater than num_executors (i.e. num_executors == num_ps + num_workers)"
    if evaluator:
        assert num_executors - num_ps > 2, "number of workers must be atleast 3 if evaluator is set to True"

    global running
    if running:
        raise RuntimeError("An experiment is currently running.")

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = experiment_utils._populate_experiment(name, 'parameter_server', 'DISTRIBUTED_TRAINING', None, description, app_id, None, None)
        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        logdir, return_dict = ps_impl._run(sc, train_fn, run_id, local_logdir=local_logdir, name=name, evaluator=evaluator)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        metric = experiment_utils._get_metric(return_dict, metric_key)

        experiment_utils._finalize_experiment(experiment_json, metric, app_id, run_id, 'FINISHED', duration, logdir, None, None)

        return logdir, return_dict
    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def mirrored(train_fn, name='no-name', local_logdir=False, description=None, evaluator=False, metric_key=None):
    """
    *Distributed Training*

    Example usage:

    >>> from hops import experiment
    >>> def mirrored_training():
    >>>    # Do all imports in the function
    >>>    import tensorflow
    >>>    # Put all code inside the train_fn function
    >>>    from hops import tensorboard
    >>>    from hops import devices
    >>>    logdir = tensorboard.logdir()
    >>>    ...MirroredStrategy()...
    >>> experiment.mirrored(mirrored_training, local_logdir=True)

    Args:
        :train_fn: contains the code where you are using MirroredStrategy.
        :name: name of the experiment
        :local_logdir: True if *tensorboard.logdir()* should be in the local filesystem, otherwise it is in HDFS
        :description: a longer description for the experiment
        :evaluator: whether to run one of the workers as an evaluator
        :metric_key: If returning a dict with multiple return values, this key should match the name of the key in the dict for the metric you want to associate with the experiment

    Returns:
        HDFS path in your project where the experiment is stored and return value from the process running as chief

    """

    num_ps = util.num_param_servers()
    assert num_ps == 0, "number of parameter servers should be 0"

    global running
    if running:
        raise RuntimeError("An experiment is currently running.")

    num_workers = util.num_executors()
    if evaluator:
        assert num_workers > 2, "number of workers must be atleast 3 if evaluator is set to True"

    start = time.time()
    sc = util._find_spark().sparkContext
    try:
        global app_id
        global experiment_json
        global run_id
        app_id = str(sc.applicationId)

        _start_run()

        experiment_utils._create_experiment_dir(app_id, run_id)

        experiment_json = experiment_utils._populate_experiment(name, 'mirrored', 'DISTRIBUTED_TRAINING', None, description, app_id, None, None)
        exp_ml_id = app_id + "_" + str(run_id)
        experiment_json = experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'INIT')

        logdir, return_dict = mirrored_impl._run(sc, train_fn, run_id, local_logdir=local_logdir, name=name, evaluator=evaluator)
        duration = experiment_utils._seconds_to_milliseconds(time.time() - start)

        metric = experiment_utils._get_metric(return_dict, metric_key)

        experiment_utils._finalize_experiment(experiment_json, metric, app_id, run_id, 'FINISHED', duration, logdir, None, None)

        return logdir, return_dict
    except:
        _exception_handler(experiment_utils._seconds_to_milliseconds(time.time() - start))
        raise
    finally:
        _end_run(sc)

def _exception_handler(duration):
    """

    Returns:

    """
    try:
        global running
        global experiment_json
        if running and experiment_json != None:
            experiment_json['state'] = "FAILED"
            experiment_json['duration'] = duration
            exp_ml_id = app_id + "_" + str(run_id)
            experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'FULL_UPDATE')
    except Exception as err:
        log.error(err)
        pass

def _exit_handler():
    """

    Returns:

    """
    try:
        global running
        global experiment_json
        if running and experiment_json != None:
            experiment_json['state'] = "KILLED"
            exp_ml_id = app_id + "_" + str(run_id)
            experiment_utils._attach_experiment_xattr(exp_ml_id, experiment_json, 'FULL_UPDATE')
    except Exception as err:
        log.error(err)
        pass

def _start_run():
    global running
    global app_id
    global run_id
    running = True
    experiment_utils._set_ml_id(app_id, run_id)

def _end_run(sc):
    global running
    global app_id
    global run_id
    run_id = run_id + 1
    running = False
    sc.setJobGroup("", "")

atexit.register(_exit_handler)
