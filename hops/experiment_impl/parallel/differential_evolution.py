"""
Differential evolution implementation
"""


import random
from collections import OrderedDict
import os

from hops import hdfs, tensorboard, devices, util
from hops.experiment_impl.util import experiment_utils
from hops.experiment import Direction

import threading
import six
import time
import copy
import json
import sys

objective_function=None
spark=None
opt_key=None
diff_evo=None
cleanup=None
fs_handle=None
local_logdir_bool=False

generation_id = 0
run_id = 0

def _get_all_accuracies(tensorboard_hdfs_logdir, args_dict, number_params):
    """
    Retrieves all accuracies from the parallel executions (each one is in a
    different file, one per combination of wrapper function parameter)

    Args:
        :tensorboard_hdfs_logdir:
        :args_dict:
        :number_params:

    Returns:

    """

    results=[]

    #Important, this must be ordered equally than _parse_to_dict function
    population_dict = diff_evo.get_dict()
    global run_id
    for i in range(number_params):
        path_to_log= tensorboard_hdfs_logdir + "/generation." + str(generation_id - 1) + "/"
        for k in population_dict:
            path_to_log+=k+'='+str(args_dict[k][i])+ '&'
        path_to_log = path_to_log[:(len(path_to_log) -1)]
        path_to_log = path_to_log + '/.metric'

        with hdfs.open_file(path_to_log, flags="r") as fi:
            metric = fi.read()
            fi.close()

        results.append(metric)

    return [float(res) for res in results]

def _execute_all(population_dict, name="no-name"):
    """
    Executes wrapper function with all values of population_dict parallely.
    Returns a list of accuracies (or metric returned in the wrapper) in the
    same order as in the population_dict.

    Args:
        :population_dict:
        :name:

    Returns:

    """
    initial_pop = copy.deepcopy(population_dict)

    number_hp_combinations=[len(v) for v in population_dict.values()][0]
    # directory for current generation


    # Do not run hyperparameter combinations that are duplicated
    # Find all duplicates and delete them
    keys = population_dict.keys()
    i=0
    while i < number_hp_combinations:
        duplicate_entries = _duplicate_entry(i, keys, population_dict, number_hp_combinations)
        if len(duplicate_entries) > 0:
            # sort entries, delete descending
            for index in duplicate_entries:
                for key in keys:
                    population_dict[key].pop(index)
            i=0
            number_hp_combinations = [len(v) for v in population_dict.values()][0]
        else:
            i+=1

    tensorboard_hdfs_logdir = _evolutionary_launch(spark, objective_function, population_dict, name=name)

    return _get_all_accuracies(tensorboard_hdfs_logdir, initial_pop, [len(v) for v in initial_pop.values()][0])


def _duplicate_entry(i, keys, population, len):
    """

    Args:
        :i:
        :keys:
        :population:
        :len:

    Returns:

    """
    hp_combinations = []
    duplicate_indices = []

    for val in range(len):
        entry=''
        for key in keys:
            entry += str(population[key][val]) + '='
        hp_combinations.append(entry)

    to_find = hp_combinations[i]
    #get the duplicate indices
    for y in range(len):
        if hp_combinations[y] == to_find and y != i:
            duplicate_indices.insert(0, y)

    return duplicate_indices

class DifferentialEvolution:
    _types = ['float', 'int', 'cat']
    _generation = 0
    _scores = []
    _ordered_population_dict = []
    _param_names = []

    def __init__(self, objective_function, parbounds, types, ordered_dict, direction=Direction.MAX, generations=10, population=10, mutation=0.5, crossover=0.7, name="no-name"):
        """

        Args:
            :objective_function:
            :parbounds:
            :types:
            :ordered_dict:
            :direction:
            :generations:
            :population:
            :mutation:
            :crossover:
            :name:
        """
        self.objective_function = objective_function
        self.parbounds = parbounds
        self.direction = direction
        self.types = types
        self.generations = generations
        self.n = population
        self.F = mutation
        self.CR = crossover
        self._ordered_population_dict = ordered_dict
        self.name = name

        global generation_id
        generation_id = 0

        self._param_names = []
        for entry in ordered_dict:
            self._param_names.append(entry)

        #self.m = -1 if maximize else 1

    # run differential evolution algorithms
    def _solve(self, root_dir):
        """

        Args:
            :root_dir:

        Returns:

        """
        # initialise generation based on individual representation
        population, bounds = self._population_initialisation()
        global fs_handle
        fs_handle = hdfs.get_fs()
        global run_id

        new_gen_best_param = None
        new_gen_best = None

        for _ in range(self.generations):

            donor_population = self._mutation(population, bounds)
            trial_population = self._recombination(population, donor_population)

            population = self._selection(population, trial_population)

            new_gen_avg = sum(self._scores)/self.n

            if self.direction.upper() == Direction.MAX:
                new_gen_best = max(self._scores)
            elif self.direction.upper() == Direction.MIN:
                new_gen_best = min(self._scores)
            else:
                raise ValueError('invalid direction: ' + self.direction)

            new_gen_best_param = self._parse_back(population[self._scores.index(new_gen_best)])

            index = 0
            for name in self._param_names:
                new_gen_best_param[index] = name + "=" + str(new_gen_best_param[index])
                index += 1

            print("Generation " + str(self._generation) + " || " + "average metric: " + str(new_gen_avg) \
                  + ", best metric: " + str(new_gen_best) + ", best parameter combination: " + str(new_gen_best_param) + "\n")

            if cleanup:
                hdfs.rmr(root_dir + '/generation.' + str(self._generation-1))

        parsed_back_population = []
        for indiv in population:
            parsed_back_population.append(self._parse_back(indiv))

        return new_gen_best_param, new_gen_best

    # define bounds of each individual depending on type
    def _individual_representation(self):
        """

        Returns:

        """
        bounds = []

        for index, item in enumerate(self.types):
            b =()
            # if categorical then take bounds from 0 to number of items
            if item == self._types[2]:
                b = (0, int(len(self.parbounds[index]) - 1))
            # if float/int then take given bounds
            else:
                b = self.parbounds[index]
            bounds.append(b)
        return bounds

    # initialise population
    def _population_initialisation(self):
        """

        Returns:

        """
        population = []
        num_parameters = len(self.parbounds)
        for i in range(self.n):
            indiv = []
            bounds = self._individual_representation()

            for i in range(num_parameters):
                indiv.append(random.uniform(bounds[i][0], bounds[i][1]))
            indiv = self._ensure_bounds(indiv, bounds)
            population.append(indiv)
        return population, bounds

    # ensure that any mutated individual is within bounds
    def _ensure_bounds(self, indiv, bounds):
        """

        Args:
            :indiv:
            :bounds:

        Returns:

        """
        indiv_correct = []

        for i in range(len(indiv)):
            par = indiv[i]

            # check if param is within bounds
            lowerbound = bounds[i][0]
            upperbound = bounds[i][1]
            if par < lowerbound:
                par = lowerbound
            elif par > upperbound:
                par = upperbound

            # check if param needs rounding
            if self.types[i] != 'float':
                par = int(round(par))
            indiv_correct.append(par)
        return indiv_correct

    # create donor population based on mutation of three vectors
    def _mutation(self, population, bounds):
        """

        Args:
            :population:
            :bounds:

        Returns:

        """
        donor_population = []
        for i in range(self.n):

            indiv_indices = list(range(0, self.n))
            indiv_indices.remove(i)

            candidates = random.sample(indiv_indices, 3)
            x_1 = population[candidates[0]]
            x_2 = population[candidates[1]]
            x_3 = population[candidates[2]]

            # substracting the second from the third candidate
            x_diff = [x_2_i - x_3_i for x_2_i, x_3_i in zip(x_2, x_3)]
            donor_vec = [x_1_i + self.F*x_diff_i for x_1_i, x_diff_i in zip (x_1, x_diff)]
            donor_vec = self._ensure_bounds(donor_vec, bounds)
            donor_population.append(donor_vec)

        return donor_population

    # recombine donor vectors according to crossover probability
    def _recombination(self, population, donor_population):
        """

        Args:
            :population:
            :donor_population:

        Returns:

        """
        trial_population = []
        for k in range(self.n):
            target_vec = population[k]
            donor_vec = donor_population[k]
            trial_vec = []
            for p in range(len(self.parbounds)):
                crossover = random.random()

                # if random number is below set crossover probability do recombination
                if crossover <= self.CR:
                    trial_vec.append(donor_vec[p])
                else:
                    trial_vec.append(target_vec[p])
            trial_population.append(trial_vec)
        return trial_population

    # select the best individuals from each generation
    def _selection(self, population, trial_population):
        """

        Args:
            :population:
            :trial_population:

        Returns:

        """
        # Calculate trial vectors and target vectors and select next generation

        if self._generation == 0:
            parsed_population = []
            for target_vec in population:
                parsed_target_vec = self._parse_back(target_vec)
                parsed_population.append(parsed_target_vec)

            parsed_population = self._parse_to_dict(parsed_population)
            self._scores = self.objective_function(parsed_population, name=self.name)

            if self.direction.upper() == Direction.MAX:
                new_gen_best = max(self._scores)
            elif self.direction.upper() == Direction.MIN:
                new_gen_best = min(self._scores)
            else:
                raise ValueError('invalid direction: ' + self.direction)

            new_gen_best_param = self._parse_back(population[self._scores.index(new_gen_best)])

            index = 0
            for name in self._param_names:
                new_gen_best_param[index] = name + "=" + str(new_gen_best_param[index])
                index += 1

        parsed_trial_population = []
        for index, trial_vec in enumerate(trial_population):
            parsed_trial_vec = self._parse_back(trial_vec)
            parsed_trial_population.append(parsed_trial_vec)

        parsed_trial_population =  self._parse_to_dict(parsed_trial_population)
        trial_population_scores = self.objective_function(parsed_trial_population, name=self.name)

        for i in range(self.n):
            trial_vec_score_i = trial_population_scores[i]
            target_vec_score_i = self._scores[i]
            if self.direction.upper() == Direction.MAX:
                if trial_vec_score_i > target_vec_score_i:
                    self._scores[i] = trial_vec_score_i
                    population[i] = trial_population[i]
            elif self.direction.upper() == Direction.MIN:
                if trial_vec_score_i < target_vec_score_i:
                    self._scores[i] = trial_vec_score_i
                    population[i] = trial_population[i]

        self._generation += 1

        return population
    # parse the converted values back to original
    def _parse_back(self, individual):
        """

        Args:
            :individual:

        Returns:

        """
        original_representation = []
        for index, parameter in enumerate(individual):
            if self.types[index] == self._types[2]:
                original_representation.append(self.parbounds[index][parameter])
            else:

                original_representation.append(parameter)

        return original_representation

    # for parallelization purposes one can parse the population from a list to a  dictionary format
    # User only has to add the parameters he wants to optimize to population_dict
    def _parse_to_dict(self, population):
        """

        Args:
            :population:

        Returns:

        """

        # reset entries
        for entry in self._ordered_population_dict:
            self._ordered_population_dict[entry] = []


        for indiv in population:
            index = 0
            for param in self._param_names:
                self._ordered_population_dict[param].append(indiv[index])
                index = index + 1

        return self._ordered_population_dict

    def get_dict(self):
        return self._ordered_population_dict

def _run(train_fn, search_dict, direction = Direction.MAX, generations=4, population=6, mutation=0.5, crossover=0.7, cleanup_generations=False, local_logdir=False, name="no-name", optimization_key=None):
    """

    Args:
        :train_fn:
        :search_dict:
        :direction:
        :generations:
        :population:
        :mutation:
        :crossover:
        :cleanup_generations:
        :local_logdir:
        :name:
        :optimization_key:

    Returns:

    """

    global run_id
    global local_logdir_bool
    local_logdir_bool = local_logdir

    global spark
    spark = util._find_spark()

    global objective_function
    objective_function = train_fn

    global cleanup
    cleanup = cleanup_generations

    global opt_key
    opt_key = optimization_key

    argcount = six.get_function_code(train_fn).co_argcount
    arg_names = six.get_function_code(train_fn).co_varnames

    ordered_arr = []

    app_id = spark.sparkContext.applicationId

    arg_lists = list(search_dict.values())
    for i in range(len(arg_lists)):
        if len(arg_lists[i]) != 2:
            raise ValueError('Boundary list must contain exactly two elements, [lower_bound, upper_bound] for float/int '
                             'or [category1, category2] in the case of strings')

    assert population > 3, 'population should be greater than 3'
    assert generations > 1, 'generations should be greater than 1'

    argIndex = 0
    while argcount != 0:
        ordered_arr.append((arg_names[argIndex], search_dict[arg_names[argIndex]]))
        argcount = argcount - 1
        argIndex = argIndex + 1

    ordered_dict = OrderedDict(ordered_arr)

    bounds_list = []
    types_list = []

    for entry in ordered_dict:
        bounds_list.append((ordered_dict[entry][0], ordered_dict[entry][1]))

        if isinstance(ordered_dict[entry][0], int):
            types_list.append('int')
        elif isinstance(ordered_dict[entry][0], float):
            types_list.append('float')
        else:
            types_list.append('cat')

    global diff_evo
    diff_evo = DifferentialEvolution(_execute_all,
                                     bounds_list,
                                     types_list,
                                     ordered_dict,
                                     direction=direction,
                                     generations=generations,
                                     population=population,
                                     crossover=crossover,
                                     mutation=mutation,
                                     name=name)

    root_dir = experiment_utils._get_experiments_dir() + "/" + str(app_id) + "_" + str(run_id)

    best_param, best_metric = diff_evo._solve(root_dir)


    param_string = ''
    for hp in best_param:
        param_string = param_string + hp + '&'
    param_string = param_string[:-1]

    best_exp_logdir, return_dict = _get_best(str(root_dir), direction)

    print('Finished Experiment \n')

    return best_exp_logdir, experiment_utils._get_params_dict(best_exp_logdir), best_metric, return_dict

def _evolutionary_launch(spark, train_fn, args_dict, name="no-name"):
    """ Run the wrapper function with each hyperparameter combination as specified by the dictionary

    Args:
        :spark_session: SparkSession object
        :train_fn: The TensorFlow function to run
        :args_dict: (optional) A dictionary containing hyperparameter values to insert as arguments for each TensorFlow job
    """

    global run_id

    sc = spark.sparkContext
    app_id = str(sc.applicationId)


    arg_lists = list(args_dict.values())
    num_executions = len(arg_lists[0])

    #Each TF task should be run on 1 executor
    nodeRDD = sc.parallelize(range(num_executions), num_executions)

    #Force execution on executor, since GPU is located on executor
    global generation_id
    global run_id

    #Make SparkUI intuitive by grouping jobs
    sc.setJobGroup(os.environ['ML_ID'], "{} | Differential Evolution, Generation: {}".format(name, generation_id))
    nodeRDD.foreachPartition(_prepare_func(app_id, generation_id, train_fn, args_dict, run_id, opt_key))

    generation_id += 1

    return experiment_utils._get_experiments_dir() + '/' + str(app_id) + "_" + str(run_id)


#Helper to put Spark required parameter iter in function signature
def _prepare_func(app_id, generation_id, train_fn, args_dict, run_id, opt_key):
    """

    Args:
        :app_id:
        :generation_id:
        :train_fn:
        :args_dict:
        :run_id:

    Returns:

    """

    def _wrapper_fun(iter):
        """

        Args:
            :iter:

        Returns:

        """

        for i in iter:
            executor_num = i

        experiment_utils._set_ml_id(app_id, run_id)

        tb_hdfs_path = ''
        hdfs_exec_logdir = ''

        t = threading.Thread(target=devices._print_periodic_gpu_utilization)
        if devices.get_num_gpus() > 0:
            t.start()

        global local_logdir_bool

        try:
            #Arguments
            if args_dict:
                param_string, params, args = experiment_utils.build_parameters(train_fn, executor_num, args_dict)
                val = _get_return_file(param_string, app_id, generation_id, run_id)
                hdfs_exec_logdir, hdfs_appid_logdir = experiment_utils._create_experiment_subdirectories(app_id, run_id, param_string, 'differential_evolution', sub_type='generation.' + str(generation_id), params=params)
                logfile = experiment_utils._init_logger(hdfs_exec_logdir)
                tb_hdfs_path, tb_pid = tensorboard._register(hdfs_exec_logdir, hdfs_appid_logdir, executor_num, local_logdir=local_logdir_bool)
                print(devices._get_gpu_info())
                print('-------------------------------------------------------')
                print('Started running task ' + param_string)
                if val is not None:
                    val = json.loads(val)
                task_start = time.time()
                if val is None:
                    val = train_fn(*args)
                task_end = time.time()
                time_str = 'Finished task ' + param_string + ' - took ' + experiment_utils._time_diff(task_start, task_end)
                print(time_str)
                experiment_utils._handle_return(val, hdfs_exec_logdir, opt_key, logfile)
                print('Returning metric ' + str(val))
                print('-------------------------------------------------------')
        except:
            raise
        finally:
            experiment_utils._cleanup(tensorboard, t)

    return _wrapper_fun

def _get_return_file(param_string, app_id, generation_id, run_id):
    """

    Args:
        :param_string:
        :app_id:
        :generation_id:
        :run_id:

    Returns:

    """
    handle = hdfs.get()
    for i in range(generation_id):
        possible_result_path = experiment_utils._get_experiments_dir() + '/' + app_id + '_' \
                               + str(run_id) + '/generation.' + str(i) + '/' + param_string + '/.outputs.json'
        if handle.exists(possible_result_path):
            return_file_contents = hdfs.load(possible_result_path)
            return return_file_contents

    return None

def _get_best(root_logdir, direction):

    min_val = sys.float_info.max
    min_logdir = None

    max_val = sys.float_info.min
    max_logdir = None

    generation_folders = hdfs.ls(root_logdir)
    generation_folders.sort()

    for generation in generation_folders:
        for individual in hdfs.ls(generation):
            invidual_files = hdfs.ls(individual, recursive=True)
            for file in invidual_files:
                if file.endswith("/.metric"):
                    val = hdfs.load(file)
                    val = float(val)

                    if val > max_val:
                        max_val = val
                        max_logdir = file[:-8]

                    if val < min_val:
                        min_val = val
                        min_logdir = file[:-8]



    if direction.upper() == Direction.MAX:
        return_dict = {}
        with hdfs.open_file(max_logdir + '/.outputs.json', flags="r") as fi:
            return_dict = json.loads(fi.read())
            fi.close()
        return max_logdir, return_dict
    else:
        return_dict = {}
        with hdfs.open_file(min_logdir + '/.outputs.json', flags="r") as fi:
            return_dict = json.loads(fi.read())
            fi.close()
        return min_logdir, return_dict