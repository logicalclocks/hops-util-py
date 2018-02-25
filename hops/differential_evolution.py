import random
from hops import hdfs
from collections import OrderedDict
import six

objective_function=None
spark_session=None
diff_evo=None

def get_accuracy(v):
    if sep in v["_c0"]:
        i = v["_c0"].find(sep)
        substr = v["_c0"][i+len(sep):]
        i = substr.find(sep)
        return [substr[:i]]
    else:
        return []

def get_all_accuracies(tensorboard_hdfs_logdir, args_dict, number_params):
    '''
    Retrieves all accuracies from the parallel executions (each one is in a
    different file, one per combination of wrapper function parameter)
    '''
    results=[]

    #Important, this must be ordered equally than _parse_to_dict function
    population_dict = diff_evo.get_dict()

    for i in range(number_params):
        path_to_log=tensorboard_hdfs_logdir+"/runId." + str(experiment.run_id - 1) + "/"
        for k in population_dict:
            path_to_log+=k+"="+str(args_dict[k][i])+"."
        path_to_log+="log"
        raw = spark.read.csv(path_to_log, sep="\n")

        r = raw.rdd.flatMap(lambda v: get_accuracy(v)).collect()
        results.extend(r)

    #print(results)
    return [float(res) for res in results]

def execute_all(population_dict):
    '''
    Executes wrapper function with all values of population_dict parallely.
    Returns a list of accuracies (or metric returned in the wrapper) in the
    same order as in the population_dict.
    '''

    from hops import experiment

    number_params=[len(v) for v in population_dict.values()][0]
    tensorboard_hdfs_logdir = experiment.launch(spark_session, objective_function, population_dict)
    return get_all_accuracies(tensorboard_hdfs_logdir, population_dict, number_params)

class DifferentialEvolution:
    _types = ['float', 'int', 'cat']
    _generation = 0
    _scores = []
    _ordered_population_dict = []
    _param_names = []

    def __init__(self, objective_function, parbounds, types, ordered_dict, direction = 'max', maxiter=10, popsize=10, mutationfactor=0.5, crossover=0.7):
        self.objective_function = objective_function
        self.parbounds = parbounds
        self.direction = direction
        self.types = types
        self.maxiter = maxiter
        self.n = popsize
        self.F = mutationfactor
        self.CR = crossover
        self._ordered_population_dict = ordered_dict

        for entry in ordered_dict:
            self._param_names.append(entry)

        #self.m = -1 if maximize else 1

    # run differential evolution algorithms
    def solve(self):
        # initialise generation based on individual representation
        population, bounds = self._population_initialisation()
        for _ in range(self.maxiter):
            donor_population = self._mutation(population, bounds)
            trial_population = self._recombination(population, donor_population)

            population = self._selection(population, trial_population)

            new_gen_avg = sum(self._scores)/self.n

            if self.direction == 'max':
                new_gen_best = max(self._scores)
            else:
                new_gen_best = min(self._scores)
            new_gen_best_param = self._parse_back(population[self._scores.index(new_gen_best)])

            print("Generation: " + str(self._generation) + " || " + "Average score: " + str(new_gen_avg)+
                  ", best score: " + str(new_gen_best) + "best param: " + str(new_gen_best_param))

        parsed_back_population = []
        for indiv in population:
            parsed_back_population.append(self._parse_back(indiv))

        return parsed_back_population, self._scores

    # define bounds of each individual depending on type
    def _individual_representation(self):
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
        # Calculate trial vectors and target vectors and select next generation

        if self._generation == 0:
            parsed_population = []
            for target_vec in population:
                parsed_target_vec = self._parse_back(target_vec)
                parsed_population.append(parsed_target_vec)

            parsed_population = self._parse_to_dict(parsed_population)
            self._scores = self.objective_function(parsed_population)

        parsed_trial_population = []
        for index, trial_vec in enumerate(trial_population):
            parsed_trial_vec = self._parse_back(trial_vec)
            parsed_trial_population.append(parsed_trial_vec)

        parsed_trial_population =  self._parse_to_dict(parsed_trial_population)
        trial_population_scores = self.objective_function(parsed_trial_population)

        for i in range(self.n):
            trial_vec_score_i = trial_population_scores[i]
            target_vec_score_i = self._scores[i]
            if self.direction == 'max':
                if trial_vec_score_i > target_vec_score_i:
                    self._scores[i] = trial_vec_score_i
                    population[i] = trial_population[i]
            else:
                if trial_vec_score_i < target_vec_score_i:
                    self._scores[i] = trial_vec_score_i
                    population[i] = trial_population[i]

        self._generation += 1

        return population
    # parse the converted values back to original
    def _parse_back(self, individual):
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

def search(spark, function, search_dict, direction = 'max', maxiter=10, popsize=10, mutationfactor=0.5, crossover=0.7):

    global spark_session
    spark_session = spark

    global objective_function
    objective_function = function

    argcount = six.get_function_code(function).co_argcount
    arg_names = six.get_function_code(function).co_varnames

    ordered_arr = []

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
            raise ValueError("Not supported value type " + entry)

    global diff_evo
    diff_evo = DifferentialEvolution(execute_all,
                                     bounds_list,
                                     types_list,
                                     ordered_dict,
                                     direction=direction,
                                     maxiter=maxiter,
                                     popsize=popsize,
                                     crossover=crossover,
                                     mutationfactor=mutationfactor)

    results = diff_evo.solve()

    print("Population: ", results[0])
    print("Scores: ", results[1])