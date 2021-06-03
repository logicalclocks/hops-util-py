"""

Model module is used for exporting, versioning, attaching metadata to models. In pipelines it should be used to query the Model Repository for the best model version to use during inference.

"""

from hops import constants, util, hdfs, project, dataset
from hops.experiment_impl.util import experiment_utils
from hops.exceptions import RestAPIError
import json
import sys
import os
import time
import six
from six import string_types

import urllib3
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)

def get_best_model(name, metric, direction):
    """
    Get the best model version by sorting on attached metadata such as model accuracy.

    For example if you run this:

    >>> from hops import model
    >>> from hops.model import Metric
    >>> model.get_best_model('mnist', 'accuracy', Metric.MAX)

    It will return the mnist version where the 'accuracy' is the highest.

    Args:
        :name: name of the model
        :metric: name of the metric to compare
        :direction: whether metric should be maximized or minimized to find the best model

    Returns:
        The best model

    Raises:
        :ModelNotFound: if the model was not found
    """

    if direction.upper() == Metric.MAX:
        direction = "desc"
    elif direction.upper() == Metric.MIN:
        direction = "asc"
    else:
        raise Exception("Invalid direction, should be Metric.MAX or Metric.MIN")

    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}

    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_MODELS_RESOURCE + \
                   "?filter_by=name_eq:" + name + "&sort_by=" + metric + ":" + direction + "&limit=1"

    response_object = util.send_request('GET', resource_url, headers=headers)

    if not response_object.ok or 'items' not in json.loads(response_object.content.decode("UTF-8")):
        raise ModelNotFound("No model with name {} and metric {} could be found.".format(name, metric))

    return json.loads(response_object.content.decode("UTF-8"))['items'][0]


def get_model(name, version, project_name=None):
    """
    Get a specific model version given a model name and a version.

    For example if you run this:

    >>> from hops import model
    >>> model.get_model('mnist', 1)

    You will get version 1 of the model 'mnist'

    Args:
        :name: name of the model
        :version: version of the model
        :project_name name of the project parent of the model. By default, this project is the current project running
        the experiment

    Returns:
        The specified model version

    Raises:
        :ModelNotFound: if the model was not found
    """

    headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
    project_id = project.project_id_as_shared(project_name)
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_MODELS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   str(name) + "_" + str(version) + "?filter_by=endpoint_id:" + project_id

    print("get model:" + resource_url)
    response_object = util.send_request('GET', resource_url, headers=headers)

    if response_object.ok:
        return response_object

    raise ModelNotFound("No model with name: {} and version {} could be found".format(name, version))


def download_model(name, version=None, project_name=None, overwrite=False):
    """
    Download from the Hopsworks Models dataset an archive (zip file) containing the model artifacts.
    You first need to use the project.connect function to connect to Hopsworks.
    If the Models dataset where the model resides is a shared dataset from another project,
    then you need to specify the name of the project that owns the Models dataset was shared from.

    For example if you run this:

    >>> from hops import model
    >>> # If connecting from an external client, you need to connect to Hopsworks
    >>> project.connect(...) # see project module for documentation
    >>> model.download_model('mnist')

    Args:
        :name: name of the model
        :version: version of the model. If omitted, all versions of the model will be included in the archive.
        :project_name name of the project parent of the model. By default, this project is the current project running
        the experiment
        :overwrite: Whether to overwrite the model archive  file if it already exists

    Returns:
        A zip file containing the model artifacts

    Raises:
        :ModelArchiveExists: if the model archive that contains the model artifacts already exists
    """
    if project_name is None:
        project_name = hdfs.project_name()

    # Check if model archive already exists and if it should be deleted, otherwise return an error
    model_dir = '/Projects/' + project_name + "/Models/" + name
    if version is not None:
        model_dir += "/" + str(version)
        name += str(version)
    archive_path = model_dir + ".zip"
    name += ".zip"
    if dataset.path_exists(archive_path):
        if overwrite:
            dataset.delete(archive_path, block=True)
        else:
            raise ModelArchiveExists("Model archive file already exists at {}. Either set overwrite=True or remove the file manually.".format(archive_path))

    print("Preparing the model archive...")
    dataset.compress(model_dir, block=True, project_name=project_name)
    print("Downloading the model archive...")
    dataset.download(archive_path, file = name)


def export(model_path, model_name, model_version=None, overwrite=False, metrics=None, description=None,
           synchronous=True, synchronous_timeout=120, project=None):
    """
    Copies a trained model to the Models directory in the project and creates the directory structure of:

    >>> Models
    >>>      |
    >>>      - model_name
    >>>                 |
    >>>                 - version_x
    >>>                 |
    >>>                 - version_y

    For example if you run this:

    >>> from hops import model
    >>> model.export("iris_knn.pkl", "irisFlowerClassifier", metrics={'accuracy': accuracy})

    It will copy the local model file "iris_knn.pkl" to /Projects/projectname/Models/irisFlowerClassifier/1/iris.knn.pkl
    on HDFS, and overwrite in case there already exists a file with the same name in the directory.

    If "model" is a directory on the local path exported by TensorFlow, and you run:

    >>> model.export("/model", "mnist", metrics={'accuracy': accuracy, 'loss': loss})

    It will copy the model directory contents to /Projects/projectname/Models/mnist/1/ , e.g the "model.pb" file and
    the "variables" directory.

    Args:
        :model_path: absolute path to the trained model (HDFS or local)
        :model_name: name of the model
        :model_version: version of the model
        :overwrite: boolean flag whether to overwrite in case a model already exists in the exported directory
        :metrics: dict of evaluation metrics to attach to model
        :description: description about the model
        :synchronous: whether to synchronously wait for the model to be indexed in the models rest endpoint
        :synchronous_timeout: max timeout in seconds for waiting for the model to be indexed
        :project: the name of the project where the model should be saved to (default: current project). Note, the project must share its 'Models' dataset and make it writeable for this client.

    Returns:
        The path to where the model was exported

    Raises:
        :ValueError: if there was an error with the model due to invalid user input
        :ModelNotFound: if the model was not found
    """

    # Make sure model name is a string, users could supply numbers
    model_name = str(model_name)

    if not isinstance(model_path, string_types):
        model_path = model_path.decode()

    if not description:
        description = 'A collection of models for ' + model_name

    project_path = hdfs.project_path(project)

    assert hdfs.exists(project_path + "Models"), "Your project is missing a dataset named Models, please create it."

    if not hdfs.exists(model_path) and not os.path.exists(model_path):
        raise ValueError("the provided model_path: {} , does not exist in HDFS or on the local filesystem".format(
            model_path))

    # make sure metrics are numbers
    if metrics:
        _validate_metadata(metrics)

    model_dir_hdfs = project_path + constants.MODEL_SERVING.MODELS_DATASET + \
                     constants.DELIMITERS.SLASH_DELIMITER + model_name + constants.DELIMITERS.SLASH_DELIMITER

    if not hdfs.exists(model_dir_hdfs):
        hdfs.mkdir(model_dir_hdfs)
        hdfs.chmod(model_dir_hdfs, "ug+rwx")

    # User did not specify model_version, pick the current highest version + 1, set to 1 if no model exists
    version_list = []
    if not model_version and hdfs.exists(model_dir_hdfs):
        model_version_directories = hdfs.ls(model_dir_hdfs)
        for version_dir in model_version_directories:
            try:
                if hdfs.isdir(version_dir):
                    version_list.append(int(version_dir[len(model_dir_hdfs):]))
            except:
                pass
        if len(version_list) > 0:
            model_version = max(version_list) + 1

    if not model_version:
        model_version = 1

    # Path to directory in HDFS to put the model files
    model_version_dir_hdfs = model_dir_hdfs + str(model_version)

    # If version directory already exists and we are not overwriting it then fail
    if not overwrite and hdfs.exists(model_version_dir_hdfs):
        raise ValueError("Could not create model directory: {}, the path already exists, "
                         "set flag overwrite=True "
                         "to remove the version directory and create the correct directory structure".format(model_version_dir_hdfs))

    # Overwrite version directory by deleting all content (this is needed for Provenance to register Model as deleted)
    if overwrite and hdfs.exists(model_version_dir_hdfs):
       hdfs.delete(model_version_dir_hdfs, recursive=True)
       hdfs.mkdir(model_version_dir_hdfs)

    # At this point we can create the version directory if it does not exist
    if not hdfs.exists(model_version_dir_hdfs):
       hdfs.mkdir(model_version_dir_hdfs)

    # Export the model files
    if os.path.exists(model_path):
        export_dir=_export_local_model(model_path, model_version_dir_hdfs, overwrite)
    else:
        export_dir=_export_hdfs_model(model_path, model_version_dir_hdfs, overwrite)

    print("Exported model " + model_name + " as version " + str(model_version) + " successfully.")

    jobName=None
    if constants.ENV_VARIABLES.JOB_NAME_ENV_VAR in os.environ:
        jobName = os.environ[constants.ENV_VARIABLES.JOB_NAME_ENV_VAR]

    kernelId=None
    if constants.ENV_VARIABLES.KERNEL_ID_ENV_VAR in os.environ:
        kernelId = os.environ[constants.ENV_VARIABLES.KERNEL_ID_ENV_VAR]

    # Attach modelName_modelVersion to experiment directory
    if project is None:
        model_project_name = hdfs.project_name()
    else :
        model_project_name = project
    experiment_project_name = hdfs.project_name()
    model_summary = { 'name': model_name, 'projectName': model_project_name, 'version': model_version, 'metrics':  metrics,
                      'experimentId': None, 'experimentProjectName': experiment_project_name,
                      'description':  description, 'jobName': jobName, 'kernelId': kernelId }
    if 'ML_ID' in os.environ:
        model_summary['experimentId'] = os.environ['ML_ID']
        # Attach link from experiment to model
        experiment_json = experiment_utils._populate_experiment_model(model_name + '_' + str(model_version), project=project)
        experiment_utils._attach_experiment_xattr(os.environ['ML_ID'], experiment_json, 'MODEL_UPDATE')

    # Attach model metadata to models version folder
    experiment_utils._attach_model_xattr(model_name + "_" + str(model_version), experiment_utils.dumps(model_summary))

    # Model metadata is attached asynchronously by Epipe, therefore this necessary to ensure following steps in a pipeline will not fail
    if synchronous:
        start_time = time.time()
        sleep_seconds = 5
        for i in range(int(synchronous_timeout/sleep_seconds)):
            try:
                time.sleep(sleep_seconds)
                print("Polling " + model_name + " version " + str(model_version) + " for model availability.")
                resp = get_model(model_name, model_version, project_name=project)
                if resp.ok:
                    print("Model now available.")
                    return
                print(model_name + " not ready yet, retrying in " + str(sleep_seconds) + " seconds.")
            except ModelNotFound:
                pass
        print("Model not available during polling, set a higher value for synchronous_timeout to wait longer.")

    return export_dir

def _export_local_model(local_model_path, model_dir_hdfs, overwrite):
    """
    Exports a local directory of model files to Hopsworks "Models" dataset

     Args:
        :local_model_path: the path to the local model files
        :model_dir_hdfs: path to the directory in HDFS to put the model files
        :overwrite: boolean flag whether to overwrite existing model files

    Returns:
           the path to the exported model files in HDFS
    """
    if os.path.isdir(local_model_path):
        if not local_model_path.endswith(constants.DELIMITERS.SLASH_DELIMITER):
            local_model_path = local_model_path + constants.DELIMITERS.SLASH_DELIMITER
        for filename in os.listdir(local_model_path):
            hdfs.copy_to_hdfs(local_model_path + filename, model_dir_hdfs, overwrite=overwrite)

    if os.path.isfile(local_model_path):
        hdfs.copy_to_hdfs(local_model_path, model_dir_hdfs, overwrite=overwrite)

    return model_dir_hdfs


def _export_hdfs_model(hdfs_model_path, model_dir_hdfs, overwrite):
    """
    Exports a hdfs directory of model files to Hopsworks "Models" dataset

     Args:
        :hdfs_model_path: the path to the model files in hdfs
        :model_dir_hdfs: path to the directory in HDFS to put the model files
        :overwrite: boolean flag whether to overwrite in case a model already exists in the exported directory

    Returns:
           the path to the exported model files in HDFS
    """
    if hdfs.isdir(hdfs_model_path):
        for file_source_path in hdfs.ls(hdfs_model_path):
            model_name = file_source_path
            if constants.DELIMITERS.SLASH_DELIMITER in file_source_path:
                last_index = model_name.rfind(constants.DELIMITERS.SLASH_DELIMITER)
                model_name = model_name[last_index + 1:]
            dest_path = model_dir_hdfs + constants.DELIMITERS.SLASH_DELIMITER + model_name
            hdfs.cp(file_source_path, dest_path, overwrite=overwrite)
    elif hdfs.isfile(hdfs_model_path):
        model_name = hdfs_model_path
        if constants.DELIMITERS.SLASH_DELIMITER in hdfs_model_path:
            last_index = model_name.rfind(constants.DELIMITERS.SLASH_DELIMITER)
            model_name = model_name[last_index + 1:]
        dest_path = model_dir_hdfs + constants.DELIMITERS.SLASH_DELIMITER + model_name
        hdfs.cp(hdfs_model_path, dest_path, overwrite=overwrite)

    return model_dir_hdfs

def _validate_metadata(metrics):
    assert type(metrics) is dict, 'provided metrics is not in a dict'
    for metric in metrics:
        try:
            assert isinstance(metric, string_types), "metrics key {} is not a string".format(str(metric))
            experiment_utils._validate_optimization_value(metrics[metric])
        except ValueError:
            raise AssertionError("{} is not a number, only numbers can be attached as metadata for models.".format(str(metrics[metric])))

class Metric:
    MAX = "MAX"
    MIN = "MIN"

class ModelNotFound(Exception):
    """This exception will be raised if the requested model could not be found"""


class ModelArchiveExists(Exception):
    """This exception will be raised if the model archive already exists."""
