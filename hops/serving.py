"""
Utility functions to export models to the Models dataset and get information about models currently being served
in the project.
"""

import os
import json
import re

from hops import hdfs, constants, util, exceptions, kafka

import logging
log = logging.getLogger(__name__)


def exists(serving_name):
    """
    Checks if there exists a serving with the given name

    Example use-case:

    >>> from hops import serving
    >>> serving.exist(serving_name)

    Args:
        :serving_name: the name of the serving

    Returns:
           True if the serving exists, otherwise false
    """
    try:
        return get_id(serving_name) is not None
    except ServingNotFound:
        log.info("No serving with name {} was found in the project {}".format(serving_name, hdfs.project_name()))
        return False


def delete(serving_name):
    """
    Deletes serving instance with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.delete("irisFlowerClassifier")

    Args:
        :serving_name: name of the serving to delete

    Returns:
        None
    """
    serving_id = get_id(serving_name)
    log.debug("Deleting serving with name: {}...".format(serving_name))
    _delete_serving_rest(serving_id)
    log.info("Serving with name: {} successfully deleted".format(serving_name))


def _delete_serving_rest(serving_id):
    """
    Makes a REST request to Hopsworks REST API for deleting a serving instance

    Args:
        :serving_id: id of the serving to delete

    Returns:
        None

    Raises:
        :RestAPIError: if there was an error with the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_DELETE
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_SERVING_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(serving_id))
    response = util.send_request(method, resource_url)

    if response.status_code != 200:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise exceptions.RestAPIError("Could not delete serving with id {} (url: {}), "
                                      "server response: \n "
                                      "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, "
                                      "user msg: {}".format(serving_id, resource_url, response.status_code,
                                                            response.reason, error_code, error_msg, user_msg))


def start(serving_name):
    """
    Starts a model serving instance with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.start("irisFlowerClassifier")

    Args:
        :serving_name: name of the serving to start

    Returns:
        None
    """
    serving_id = get_id(serving_name)
    log.debug("Starting serving with name: {}...".format(serving_name))
    _start_or_stop_serving_rest(serving_id, constants.MODEL_SERVING.SERVING_ACTION_START)
    log.info("Serving with name: {} successfully started".format(serving_name))


def stop(serving_name):
    """
    Stops a model serving instance with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.stop("irisFlowerClassifier")

    Args:
        :serving_name: name of the serving to stop

    Returns:
        None
    """
    serving_id = get_id(serving_name)
    log.debug("Stopping serving with name: {}...".format(serving_name))
    _start_or_stop_serving_rest(serving_id, constants.MODEL_SERVING.SERVING_ACTION_STOP)
    log.info("Serving with name: {} successfully stopped".format(serving_name))


def _start_or_stop_serving_rest(serving_id, action):
    """
    Makes a REST request to Hopsworks REST API for starting/stopping a serving instance

    Args:
        :serving_id: id of the serving to start/stop
        :action: the action to perform (start or stop)

    Returns:
        None

    Raises:
        :RestAPIError: if there was an error with the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_SERVING_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER
                    + str(serving_id) + constants.MODEL_SERVING.SERVING_START_OR_STOP_PATH_PARAM + action)
    response = util.send_request(method, resource_url)

    if response.status_code != 200:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise exceptions.RestAPIError("Could not perform action {} on serving with id {} (url: {}), "
                                      "server response: \n "
                                      "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, "
                                      "user msg: {}".format(action, serving_id, resource_url, response.status_code,
                                                            response.reason, error_code, error_msg, user_msg))


def create_or_update(serving_name, model_path, model_server, model_version=1, artifact_version=None, predictor=None, transformer=None, kserve=None,
                     batching_enabled=False, topic_name="CREATE", num_partitions=1, num_replicas=1, inference_logging=constants.MODEL_SERVING.INFERENCE_LOGGING_ALL,
                     instances=1, transformer_instances=None, predictor_resource_config=None):
    """
    Creates a serving in Hopsworks if it does not exist, otherwise update the existing one.
    In case model server is not specified, it is inferred from the artifact files.
    If a transformer is specified, KServe is enabled by default.

    Example use-case:

    >>> from hops import serving
    >>> serving.create_or_update("mnist", "/Models/mnist")

    Args:
        :serving_name: name of the serving to create
        :model_path: path to the model directory
        :model_server: name of the model server to deploy, e.g "TENSORFLOW_SERVING" or "PYTHON"
        :model_version: version of the model to serve
        :artifact_version: version of the artifact to serve (Kubernetes only), e.g "CREATE", "MODEL-ONLY" or version number.
        :predictor: path to the predictor script (python script implementing the Predict class).
        :transformer: path to the transformer script (python script implementing the Transformer class).
        :kserve: boolean flag whether to serve the model using KServe serving tool
        :batching_enabled: boolean flag whether to enable batching for the inference requests
        :topic_name: name of the kafka topic for inference logging, e.g "CREATE" to create a new one, "NONE" to not use kafka topic or an existent topic name
        :num_partitions: if a new kafka topic is to created, number of partitions of the new topic
        :num_replicas: if a new kafka topic is to created, replication factor of the new topic
        :inference_logging: inference data to log into the Kafka topic, e.g "MODEL_INPUTS", "PREDICTIONS" or "ALL"
        :instances: the number of serving instances (the more instances the more inference requests can
        be served in parallel)
        :transformer_instances: the number of serving instances (the more instances the more inference requests can
        be served in parallel)
        :predictor_resource_config: dict for setting resource configuration parameters required to serve the model, for
        example {'memory': 2048, 'cores': 1, 'gpus': 0}. Currently only supported if Hopsworks is deployed with Kubernetes installed.

    Returns:
          None
    """
    model_path = hdfs._expand_path(model_path)
    if transformer is not None and kserve is None:
        kserve = True

    _validate_user_serving_input(serving_name, model_path, model_server, model_version, artifact_version, predictor, transformer, kserve,
                                 batching_enabled, topic_name, num_partitions, num_replicas, inference_logging, instances, transformer_instances,
                                 predictor_resource_config)
    model_path = hdfs.get_plain_path(model_path)
    serving_id = get_id(serving_name)
    log.debug("Creating serving {} for artifact {} ...".format(serving_name, model_path))
    _create_or_update_serving_rest(serving_id, serving_name, model_path, model_server, model_version, artifact_version, predictor, transformer, kserve,
                                   batching_enabled, topic_name, num_partitions, num_replicas, inference_logging, instances, transformer_instances,
                                   predictor_resource_config)
    log.info("Serving {} successfully created".format(serving_name))


def _validate_user_serving_input(serving_name, model_path, model_server, model_version, artifact_version, predictor, transformer, kserve,
                                 batching_enabled, topic_name, num_partitions, num_replicas, inference_logging, instances, transformer_instances,
                                 predictor_resource_config):
    """
    Validate user input on the client side before sending REST call to Hopsworks (additional validation will be done
    in the backend)

    Args:
        :serving_name: the name of the serving to create
        :model_path: path to the model directory
        :model_server: name of the model server to deploy, e.g "TENSORFLOW_SERVING" or "PYTHON"
        :model_version: version of the model to serve
        :artifact_version: version of the artifact to serve
        :predictor: path to the predictor script
        :transformer: path to the transformer script
        :kserve: boolean flag whether to serve the model using KServe serving tool
        :batching_enabled: boolean flag whether to enable batching for inference requests to the serving
        :topic_name: name of the kafka topic for inference logging, e.g "CREATE" to create a new one, "NONE" to not use kafka topic or an existent topic name
        :num_partitions: if a new kafka topic is to created, number of partitions of the new topic
        :num_replicas: if a new kafka topic is to created, replication factor of the new topic
        :inference_logging: inference data to log into the Kafka topic, e.g "MODEL_INPUTS", "PREDICTIONS" or "ALL"
        :instances: the number of serving instances (the more instances the more inference requests can
        be served in parallel)
        :transformer_instances: the number of transformer instances (the more instances the more inference requests can
        be served in parallel)
        :predictor_resource_config: dict for setting resource configuration parameters required to serve the model, for
        example {'memory': 2048, 'cores': 1, 'gpus': 0}. Currently only supported if Hopsworks is deployed with Kubernetes installed.

    Returns:
        None

    Raises:
        :ValueError: if the serving input failed the validation
    """
    name_pattern = re.compile("^[a-zA-Z0-9]+$")
    if len(serving_name) > 256 or serving_name == "" or not name_pattern.match(serving_name):
        raise ValueError("Name of serving cannot be empty, cannot exceed 256 characters and must match the regular "
                         "expression: ^[a-zA-Z0-9]+$, the provided name: {} is not valid".format(serving_name))
    if not hdfs.exists(model_path):
        raise ValueError("The model/artifact path must exist in HDFS, the provided path: {} "
                         "does not exist".format(model_path))
    if model_server is None:
        raise ValueError("Model server not provided, supported "
                         "model servers are: {}".format(",".join(constants.MODEL_SERVING.MODEL_SERVERS)))
    if model_server not in constants.MODEL_SERVING.MODEL_SERVERS:
        raise ValueError("The provided model_server: {} is not supported, supported "
                         "model servers are: {}".format(model_server, ",".join(constants.MODEL_SERVING.MODEL_SERVERS)))
    if inference_logging is not None and inference_logging not in constants.MODEL_SERVING.INFERENCE_LOGGING_MODES:
        raise ValueError("The provided inference_logging: {} is not supported, supported "
                         "inference logging modes are: {}".format(inference_logging, ",".join(constants.MODEL_SERVING.INFERENCE_LOGGING_MODES)))
    if not isinstance(model_version, int):
        raise ValueError("The model version must be an integer, the provided version is not: {}".format(model_version))
    if model_server == constants.MODEL_SERVING.MODEL_SERVER_TENSORFLOW_SERVING:
        if not isinstance(num_replicas, int):
            raise ValueError("Number of kafka topic replicas must be an integer, the provided num replicas "
                             "is not: {}".format(model_version))
        if not isinstance(num_partitions, int):
            raise ValueError("Number of kafka topic partitions must be an integer, the provided num partitions "
                             "is not: {}".format(num_partitions))
        if not isinstance(batching_enabled, bool):
            raise ValueError("Batching enabled must be a boolean, the provided value "
                             "is not: {}".format(batching_enabled))
        if predictor is not None:
            raise ValueError("Predictors are not supported with Tensorflow Serving")
        if kserve and batching_enabled:
            raise ValueError("Batching requests is currently not supported in KServe deployments")

    if not isinstance(instances, int):
        raise ValueError("The number of serving instances must be an integer, "
                         "the provided version is not: {}".format(instances))

    if not kserve:
        if inference_logging is not None and inference_logging != constants.MODEL_SERVING.INFERENCE_LOGGING_ALL:
            raise ValueError("Fine-grained inference logging is only supported in KServe deployments")
        if topic_name is not None and topic_name != "NONE" and inference_logging != constants.MODEL_SERVING.INFERENCE_LOGGING_ALL:
            raise ValueError("Inference logging mode 'ALL' is the only mode supported for non-KServe deployments")

    if kserve:
        if topic_name is not None and topic_name != "NONE" and inference_logging is None:
            raise ValueError("Inference logging must be defined. Supported inference "
                             "logging modes are: {}".format(",".join(constants.MODEL_SERVING.INFERENCE_LOGGING_MODES)))

    if predictor_resource_config is not None:
        if type(predictor_resource_config) is not dict:
            raise ValueError("predictor_resource_config must be a dict.")
        if 'memory' not in predictor_resource_config or 'cores' not in predictor_resource_config:
            raise ValueError("predictor_resource_config must contain the keys 'memory' and 'cores'")


def _create_or_update_serving_rest(serving_id, serving_name, model_path, model_server, model_version, artifact_version, predictor, transformer, kserve,
                                   batching_enabled, topic_name, num_partitions, num_replicas, inference_logging, instances, transformer_instances,
                                   predictor_resource_config):
    """
    Makes a REST request to Hopsworks for creating or updating a model serving instance

    Args:
        :serving_id: the id of the serving in case of UPDATE, if serving_id is None, it is a CREATE operation.
        :serving_name: the name of the serving to create
        :model_path: path to the model directory
        :model_server: name of the model server to deploy, e.g "TENSORFLOW_SERVING" or "PYTHON"
        :model_version: version of the model to serve
        :artifact_version: version of the artifact to serve
        :predictor: path to the predictor script
        :transformer: path to the transformer script
        :kserve: boolean flag whether to serve the model using KServe serving tool
        :batching_enabled: boolean flag whether to enable batching for inference requests to the serving
        :topic_name: name of the kafka topic for inference logging, e.g "CREATE" to create a new one, "NONE" to not use kafka topic or an existent topic name
        :num_partitions: if a new kafka topic is to created, number of partitions of the new topic
        :num_replicas: if a new kafka topic is to created, replication factor of the new topic
        :inference_logging: inference data to log into the Kafka topic, e.g "MODEL_INPUTS", "PREDICTIONS" or "ALL"
        :instances: the number of serving instances (the more instances the more inference requests can
        be served in parallel)
        :transformer_instances: the number of transformer instances (the more instances the more inference requests can
        be served in parallel)
        :predictor_resource_config: dict for setting resource configuration parameters required to serve the model, for
        example {'memory': 2048, 'cores': 1, 'gpus': 0}. Currently only supported if Hopsworks is deployed with Kubernetes installed.

    Returns:
        None

    Raises:
        :RestAPIError: if there was an error with the REST call to Hopsworks
    """

    serving_tool = constants.MODEL_SERVING.SERVING_TOOL_KSERVE if kserve else constants.MODEL_SERVING.SERVING_TOOL_DEFAULT

    json_contents = {
        constants.REST_CONFIG.JSON_SERVING_NAME: serving_name,
        constants.REST_CONFIG.JSON_SERVING_MODEL_PATH: model_path,
        constants.REST_CONFIG.JSON_SERVING_MODEL_VERSION: model_version,
        constants.REST_CONFIG.JSON_SERVING_ARTIFACT_VERSION: artifact_version,
        constants.REST_CONFIG.JSON_SERVING_PREDICTOR: predictor,
        constants.REST_CONFIG.JSON_SERVING_TRANSFORMER: transformer,
        constants.REST_CONFIG.JSON_SERVING_MODEL_SERVER: model_server,
        constants.REST_CONFIG.JSON_SERVING_TOOL: serving_tool,
        constants.REST_CONFIG.JSON_SERVING_KAFKA_TOPIC_DTO: {
            constants.REST_CONFIG.JSON_KAFKA_TOPIC_NAME: topic_name,
            constants.REST_CONFIG.JSON_KAFKA_NUM_PARTITIONS: num_partitions,
            constants.REST_CONFIG.JSON_KAFKA_NUM_REPLICAS: num_replicas
        },
        constants.REST_CONFIG.JSON_SERVING_INFERENCE_LOGGING: inference_logging,
        constants.REST_CONFIG.JSON_SERVING_REQUESTED_INSTANCES: instances,
        constants.REST_CONFIG.JSON_SERVING_REQUESTED_TRANSFORMER_INSTANCES: transformer_instances,
        constants.REST_CONFIG.JSON_SERVING_PREDICTOR_RESOURCE_CONFIG: predictor_resource_config
    }
    if serving_id is not None:
        json_contents[constants.REST_CONFIG.JSON_SERVING_ID] = serving_id
    if model_server == constants.MODEL_SERVING.MODEL_SERVER_TENSORFLOW_SERVING:
        json_contents[constants.REST_CONFIG.JSON_SERVING_BATCHING_ENABLED] = batching_enabled
    if artifact_version == "CREATE":
        json_contents[constants.REST_CONFIG.JSON_SERVING_ARTIFACT_VERSION] = -1
    elif artifact_version == "MODEL-ONLY":
        json_contents[constants.REST_CONFIG.JSON_SERVING_ARTIFACT_VERSION] = 0
    if topic_name is None or topic_name == "NONE":
        json_contents[constants.REST_CONFIG.JSON_SERVING_INFERENCE_LOGGING] = None

    json_embeddable = json.dumps(json_contents)
    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    method = constants.HTTP_CONFIG.HTTP_PUT
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_SERVING_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER)
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)

    if response.status_code != 201 and response.status_code != 200:
        response_object = response.json()
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise exceptions.RestAPIError("Could not create or update serving (url: {}), server response: \n "
                                      "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, "
                                      "user msg: {}".format(resource_url, response.status_code, response.reason,
                                                            error_code, error_msg, user_msg))


def get(serving_name):
    """
    Gets the serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get(serving_name)

    Args:
        :serving_name: name of the serving to get

    Returns:
         the serving, None if Serving does not exist
    """
    try:
        servings = get_all()
        serving = _find_serving_with_name(serving_name, servings)
        return serving
    except ServingNotFound:
        return None


def get_id(serving_name):
    """
    Gets the id of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_id(serving_name)

    Args:
        :serving_name: name of the serving to get the id for

    Returns:
         the id of the serving, None if Serving does not exist
    """
    try:
        servings = get_all()
        serving = _find_serving_with_name(serving_name, servings)
        return serving.id
    except ServingNotFound:
        return None


def get_model_path(serving_name):
    """
    Gets the artifact path of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_model_path(serving_name)

    Args:
        :serving_name: name of the serving to get the artifact path for

    Returns:
         the artifact path of the serving (model path in case of tensorflow, or python script in case of SkLearn)
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.model_path


def get_model_name(serving_name):
    """
    Gets the name of the model served by a given serving instance

    Example use-case:

    >>> from hops import serving
    >>> serving.get_model_name(serving_name)

    Args:
        :serving_name: name of the serving to get the model name for

    Returns:
         the name of the model being served
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.model_version


def get_model_version(serving_name):
    """
    Gets the version of the model served by a given serving instance

    Example use-case:

    >>> from hops import serving
    >>> serving.get_model_version(serving_name)

    Args:
        :serving_name: name of the serving to get the version for

    Returns:
         the version of the model being served
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.model_version


def get_artifact_version(serving_name):
    """
    Gets the version of the artifact served by a given serving instance

    Example use-case:

    >>> from hops import serving
    >>> serving.get_artifact_version(serving_name)

    Args:
        :serving_name: name of the serving to get the version for

    Returns:
         the version of the artifact being served
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.artifact_version


def get_predictor(serving_name):
    """
    Gets the predictor filename used in a given serving instance

    Example use-case:

    >>> from hops import serving
    >>> serving.get_predictor(serving_name)

    Args:
        :serving_name: name of the serving to get the predictor for

    Returns:
         the predictor filename
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.predictor


def get_transformer(serving_name):
    """
    Gets the transformer filename used in a given serving instance

    Example use-case:

    >>> from hops import serving
    >>> serving.get_transformer(serving_name)

    Args:
        :serving_name: name of the serving to get the transformer for

    Returns:
         the transformer filename
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.transformer


def get_model_server(serving_name):
    """
    Gets the type of model server of the serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_model_server(serving_name)

    Args:
        :serving_name: name of the serving

    Returns:
         the model server (e.g Tensorflow Serving or Python)
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.model_server


def get_serving_tool(serving_name):
    """
    Gets the serving tool of the serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_serving_tool(serving_name)

    Args:
        :serving_name: name of the serving

    Returns:
         the serving tool (e.g DEFAULT or KSERVE)
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.serving_tool


def get_available_instances(serving_name):
    """
    Gets the number of available instances of the serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_available_instances(serving_name)

    Args:
        :serving_name: name of the serving

    Returns:
         number of available replicas (e.g int or (int, int) if the serving includes a transformer)
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.available_instances if serving.available_transformer_instances is None \
        else (serving.available_instances, serving.available_transformer_instances)


def get_kafka_topic(serving_name):
    """
    Gets the kafka topic name of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_kafka_topic(serving_name)

    Args:
        :serving_name: name of the serving to get the kafka topic name for

    Returns:
         the kafka topic name of the serving
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.kafka_topic_dto.name


def get_status(serving_name):
    """
    Gets the status of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_status(serving_name)

    Args:
        :serving_name: name of the serving to get the status for

    Returns:
         the status of the serving
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.status


def get_predictor_resource_config(serving_name):
    """
    Gets the predictor resource config of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_predictor_resource_config(serving_name)

    Args:
        :serving_name: name of the serving to get the predictor resource config for

    Returns:
         the status of the serving
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    return serving.predictor_resource_config


def get_internal_endpoints(serving_name):
    """
    Gets the internal endpoints of a serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.get_internal_endpoints(serving_name)

    Args:
        :serving_name: name of the serving to get the internal endoints for

    Returns:
         internal endpoints
    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)    
    if serving.internal_path is None:
        return None

    protocol = "http"
    endpoints = []
    for ip in serving.internal_ips:
        endpoints.append(protocol + "://" + ip + ":" + serving.internal_port)

    return endpoints


def describe(serving_name):
    """
    Describes the serving with a given name

    Example use-case:

    >>> from hops import serving
    >>> serving.describe(serving_name)

    Args:
        :serving_name: name of the serving

    """
    servings = get_all()
    serving = _find_serving_with_name(serving_name, servings)
    log.info(', '.join("%s: %s" % item for item in vars(serving).items()))


def get_all():
    """
    Gets the list of servings for the current project

    Example:

    >>> from hops import serving
    >>> servings = serving.get_all()
    >>> servings[0].name

    Returns:
         list of servings
    """
    return _parse_json_servings(_get_servings_rest())


def _find_serving_with_name(serving_name, servings):
    """
    Finds a serving with a given name from a list of servings (O(N))

    Args:
        :serving_name: name of the serving to look for
        :servings: the list of servings to look through

    Returns:
           serving with the given name

    Raises:
        :ServingNotFound: if the requested serving could not be found
    """
    serving_names = []
    for serving in servings:
        if serving.name == serving_name:
            return serving
        serving_names.append(serving.name)
    serving_names_str = ",".join(serving_names)
    raise ServingNotFound("No serving with name: {} could be found among the list of "
                          "available servings: {}".format(serving_name, serving_names_str))


def _parse_json_servings(json_servings):
    """
    Parses a list of JSON servings into Serving Objects

    Args:
        :json_servings: the list of JSON servings

    Returns:
           a list of Serving Objects
    """
    return list(map(lambda json_serving: Serving(json_serving), json_servings))


def _get_servings_rest():
    """
    Makes a REST request to Hopsworks to get a list of all servings in the current project

    Returns:
         JSON response parsed as a python dict

    Raises:
        :RestAPIError: if there was an error with the REST call to Hopsworks
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_SERVING_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER)
    response = util.send_request(method, resource_url)
    response_object = response.json()
    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise exceptions.RestAPIError("Could not fetch list of servings from Hopsworks REST API (url: {}), "
                                      "server response: \n "
                                      "HTTP code: {}, HTTP reason: {}, error code: {}, "
                                      "error msg: {}, user msg: {}".format(resource_url, response.status_code, response.reason,
                                                                           error_code, error_msg, user_msg))
    return response_object


def make_inference_request(serving_name, data, verb=":predict"):
    """
    Submit an inference request

    Example use-case:

    >>> from hops import serving
    >>> serving.make_inference_request("irisFlowerClassifier", [[1,2,3,4]], ":predict")

    Args:
        :serving_name: name of the model being served
        :data: data/json to send to the serving
        :verb: type of request (:predict, :classify, or :regress)

    Returns:
        the JSON response
    """
    return _make_inference_request_rest(serving_name, data, verb)


def _make_inference_request_rest(serving_name, data, verb):
    """
    Makes a REST request to Hopsworks for submitting an inference request to the serving instance

    Args:
        :serving_name: name of the model being served
        :data: data/json to send to the serving
        :verb: type of request (:predict, :classify, or :regress)

    Returns:
        the JSON response

    Raises:
        :RestAPIError: if there was an error with the REST call to Hopsworks
    """
    json_embeddable = json.dumps(data)

    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    if constants.ENV_VARIABLES.SERVING_API_KEY_ENV_VAR in os.environ:
        headers[constants.HTTP_CONFIG.HTTP_AUTHORIZATION] = "ApiKey " + os.environ[constants.ENV_VARIABLES.SERVING_API_KEY_ENV_VAR]

    method = constants.HTTP_CONFIG.HTTP_POST
    resource_url = (constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_INFERENCE_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    constants.REST_CONFIG.HOPSWORKS_MODELS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER +
                    serving_name + verb)
    response = util.send_request(method, resource_url, data=json_embeddable, headers=headers)
    response_object = response.json()
    error_code, error_msg, user_msg = util._parse_rest_error(response_object)

    if response.status_code != 201 and response.status_code != 200:
        raise exceptions.RestAPIError("Could not create or update serving (url: {}), server response: \n "
                                      "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, "
                                      "user msg: {}".format(resource_url, response.status_code, response.reason,
                                                            error_code, error_msg, user_msg))
    return response_object


class Serving(object):
    """
    Represents a model being served in Hopsworks
    """

    def __init__(self, serving_json):
        """
        Initialize the serving from JSON payload returned by Hopsworks REST API

        Args:
            :feature_json: JSON data about the feature returned from Hopsworks REST API
        """
        self.id = serving_json[constants.REST_CONFIG.JSON_SERVING_ID]
        self.name = serving_json[constants.REST_CONFIG.JSON_SERVING_NAME]
        self.model_path = serving_json[constants.REST_CONFIG.JSON_SERVING_MODEL_PATH]
        self.model_name = serving_json[constants.REST_CONFIG.JSON_SERVING_MODEL_NAME]
        self.model_version = serving_json[constants.REST_CONFIG.JSON_SERVING_MODEL_VERSION]
        self.artifact_version = serving_json[constants.REST_CONFIG.JSON_SERVING_ARTIFACT_VERSION] \
            if constants.REST_CONFIG.JSON_SERVING_ARTIFACT_VERSION in serving_json \
            else None
        self.predictor = serving_json[constants.REST_CONFIG.JSON_SERVING_PREDICTOR] \
            if constants.REST_CONFIG.JSON_SERVING_PREDICTOR in serving_json \
            else None
        self.transformer = serving_json[constants.REST_CONFIG.JSON_SERVING_TRANSFORMER] \
            if constants.REST_CONFIG.JSON_SERVING_TRANSFORMER in serving_json \
            else None
        self.model_server = serving_json[constants.REST_CONFIG.JSON_SERVING_MODEL_SERVER]
        self.serving_tool = serving_json[constants.REST_CONFIG.JSON_SERVING_TOOL]
        self.requested_instances = serving_json[constants.REST_CONFIG.JSON_SERVING_REQUESTED_INSTANCES]
        self.available_instances = serving_json[constants.REST_CONFIG.JSON_SERVING_AVAILABLE_INSTANCES] \
            if constants.REST_CONFIG.JSON_SERVING_AVAILABLE_INSTANCES in serving_json \
            else 0
        self.available_transformer_instances = serving_json[constants.REST_CONFIG.JSON_SERVING_AVAILABLE_TRANSFORMER_INSTANCES] \
            if constants.REST_CONFIG.JSON_SERVING_AVAILABLE_TRANSFORMER_INSTANCES in serving_json \
            else None
        if constants.REST_CONFIG.JSON_SERVING_PREDICTOR_RESOURCE_CONFIG in serving_json:
            self.predictor_resource_config = serving_json[constants.REST_CONFIG.JSON_SERVING_PREDICTOR_RESOURCE_CONFIG]
        self.creator = serving_json[constants.REST_CONFIG.JSON_SERVING_CREATOR]
        self.created = serving_json[constants.REST_CONFIG.JSON_SERVING_CREATED]
        self.status = serving_json[constants.REST_CONFIG.JSON_SERVING_STATUS]

        if constants.REST_CONFIG.JSON_SERVING_KAFKA_TOPIC_DTO in serving_json:
            self.kafka_topic_dto = kafka.KafkaTopicDTO(serving_json[constants.REST_CONFIG.JSON_SERVING_KAFKA_TOPIC_DTO])

        if constants.REST_CONFIG.JSON_SERVING_EXTERNAL_IP in serving_json:
            self.external_ip = serving_json[constants.REST_CONFIG.JSON_SERVING_EXTERNAL_IP]
        if constants.REST_CONFIG.JSON_SERVING_EXTERNAL_PORT in serving_json:
            self.external_port = serving_json[constants.REST_CONFIG.JSON_SERVING_EXTERNAL_PORT]
        if constants.REST_CONFIG.JSON_SERVING_INTERNAL_IPS in serving_json:
            self.internal_ips = serving_json[constants.REST_CONFIG.JSON_SERVING_INTERNAL_IPS]
        if constants.REST_CONFIG.JSON_SERVING_INTERNAL_PORT in serving_json:
            self.internal_port = serving_json[constants.REST_CONFIG.JSON_SERVING_INTERNAL_PORT]
        if constants.REST_CONFIG.JSON_SERVING_INTERNAL_PATH in serving_json:
            self.internal_path = serving_json[constants.REST_CONFIG.JSON_SERVING_INTERNAL_PATH]


class ServingNotFound(Exception):
    """This exception will be raised if the requested serving could not be found"""
