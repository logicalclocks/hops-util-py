"""
A module for working with Hopsworks datasets.
"""

import re
import os
import ntpath
import math
import json
import time

from hops import constants, util, project
from hops.exceptions import RestAPIError

import logging
log = logging.getLogger(__name__)

class HTTPUpload:
    size_last_chunk = 0
    DEFAULT_FLOW_STANDARD_CHUNK_SIZE = 1048576
    flow_standard_chunk_size = DEFAULT_FLOW_STANDARD_CHUNK_SIZE
    resource = None
    file = None
    f = None
    params = {}

    def __init__(self, file, path, flow_standard_chunk_size=DEFAULT_FLOW_STANDARD_CHUNK_SIZE):
        self.resource = constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        constants.REST_CONFIG.HOPSWORKS_DATASETS_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        "upload" + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        path

        self.file = file
        if not flow_standard_chunk_size:
            self.flow_standard_chunk_size = self.DEFAULT_FLOW_STANDARD_CHUNK_SIZE
        else:
            self.flow_standard_chunk_size = flow_standard_chunk_size
        self._add_flow_params()

    def _add_flow_params(self):
        self.params['templateId'] = -1
        self.params['flowChunkNumber'] = 1
        self.params['flowChunkSize'] = self.flow_standard_chunk_size
        self.params['flowTotalSize'] = self._calculate_file_size(self.file)
        self.params['flowCurrentChunkSize'] = self.flow_standard_chunk_size
        self.size_last_chunk = self.params['flowTotalSize'] % self.flow_standard_chunk_size
        self.params['flowIdentifier'] = str(self.params['flowTotalSize']) \
            + '-' \
            + re.sub("[^0-9A-Za-z_-]", "", self.file)
        self.params['flowFilename'] = self._path_leaf(self.file)
        self.params['flowRelativePath'] = self.file
        self.params['flowTotalChunks'] = self._calculate_total_chunks(self.flow_standard_chunk_size,
                                                                      self.params['flowTotalSize'])

    @staticmethod
    def _calculate_file_size(file):
        return os.path.getsize(file)

    @staticmethod
    def _calculate_size_last_chunk(chunk_size, file_size):
        return file_size % chunk_size

    @staticmethod
    def _calculate_total_chunks(chunk_size, file_size):
        chunks = math.ceil(file_size / chunk_size)
        if chunks == 0:
            chunks = 1
        return chunks

    def _next(self):
        if self.params['flowChunkNumber'] > self.params['flowTotalChunks']:
            raise Exception

        chunk_size = self.params['flowChunkSize']
        if self.params['flowChunkNumber'] == self.params['flowTotalChunks']:
            if chunk_size >= self.size_last_chunk:
                chunk_size = self.size_last_chunk
            else:
                chunk_size += self.size_last_chunk
        if self.params['flowTotalSize'] < self.params['flowChunkSize']:
            chunk_size = self.size_last_chunk

        chunk = self._read_chunk(chunk_size)
        self.params['flowCurrentChunkSize'] = chunk_size

        # Upload chunk
        response = util.send_request(constants.HTTP_CONFIG.HTTP_POST,
                                     resource="/" + self.resource,
                                     data=self.params,
                                     files={'file': (self.file, chunk)})

        response_object = response.json()
        if response.status_code >= 400:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not perform action on job's execution (url: {}), server response: \n "
                               "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                                self.resource, response.status_code, response.reason, error_code, error_msg, user_msg))

        progress = round(self.params['flowChunkNumber'] / self.params['flowTotalChunks'] * 100, 3)
        log.info("Progress: " + str(progress) + "%")
        self.params['flowChunkNumber'] += 1

    def _read_chunk(self, chunk_size=1024):
        data = self.f.read(chunk_size)
        return data

    @staticmethod
    def _path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    def upload(self):
        with open(self.file, "rb") as self.f:
            while self.params['flowChunkNumber'] <= self.params['flowTotalChunks']:
                self._next()


class HTTPDownload:
    # stream=True and chunk_size=None will read data as it arrives in whatever size the chunks are received
    DEFAULT_DOWNLOAD_CHUNK_SIZE = None

    def __init__(self, path, file, chunk_size=DEFAULT_DOWNLOAD_CHUNK_SIZE):
        self.resource = constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        os.environ[constants.ENV_VARIABLES.HOPSWORKS_PROJECT_ID_ENV_VAR] + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        constants.REST_CONFIG.HOPSWORKS_DATASETS_RESOURCE + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        "download" + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        "with_auth" + \
                        constants.DELIMITERS.SLASH_DELIMITER + \
                        path + \
                        constants.DELIMITERS.QUESTION_MARK_DELIMITER + \
                        "type=DATASET"

        self.file = file
        if not chunk_size:
            self.chunk_size = self.DEFAULT_DOWNLOAD_CHUNK_SIZE
        else:
            self.chunk_size = chunk_size

    def download(self):
        with util.send_request(constants.HTTP_CONFIG.HTTP_GET,
                               resource="/" + self.resource,
                               stream=True) as response:

            if response.status_code // 100 != 2:
                error_code, error_msg, user_msg = util._parse_rest_error(response.json())
                raise RestAPIError("Could not perform action on job's execution (url: {}), server response: \n "
                                   "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                                    self.resource, response.status_code, response.reason, error_code, error_msg, user_msg))

            with open(self.file, "wb") as f:
                downloaded = 0
                file_size = response.headers.get('Content-Length')
                if not file_size:
                    log.debug("Downloading file ...", end=" ")
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if file_size:
                        progress = round(downloaded / int(file_size) * 100, 3)
                        log.info("Progress: " + str(progress) + "%")
                if not file_size:
                    log.debug("DONE")
    

def upload(file, remote_path, chunk_size=None):
    """
    Upload data to a project's dataset by setting the path to the local file to be uploaded to a remote_path in a
    dataset. The file is split into chunks which are uploaded sequentially.
    If you run this method more than once for the same file and remote_path, if the file already exists in Hopsworks
    it will be overwritten.

    Example usage:

    >>> from hops import dataset
    >>> dataset.upload("/tmp/mylocalfile.txt", "Resources/uploaded_files_dir")

    Args:
        :file: the absolute path or the filename of the file to be uploaded.
        :remote_path: the dataset or the path to the folder in the dataset to upload the file.
        :chunk_size: (Optional) chunk size used to upload the file

    Returns:
        None
    """
    HTTPUpload(file, remote_path, chunk_size).upload()


def download(remote_path, file, chunk_size=None):
    """
    Download a file from a project's dataset by specifying the path to the remote file and a local path where the
    file is downloaded. The remote file path must include the project name as shown in the example. It is possible
    to download files from shared projects by setting the shared project name. If the chunk size is specified, the remote
    file is downloaded in chunks of similar size. Otherwise, chunks can have any size and are read as they are received.

    Example usage:

    >>> from hops import dataset
    >>> dataset.download("Projects/project_name/Resources/myremotefile.txt", "tmp/mylocalfile.txt")

    Args:
        :remote_path: the path to the remote file in the dataset
        :file: the absolute or relative path where the file is downloaded
        :chunk_size: (Optional) chunk size used to download the file

    Returns:
        None
    """
    HTTPDownload(remote_path, file, chunk_size).download()


def get_path_info(remote_path, project_name=None):
    """
    Check if file exists.

    Example usage:

    >>> from hops import dataset
    >>> dataset.get_path_info("Projects/project_name/Resources/myremotefile.txt")

    Args:
        :remote_path: the path to the remote file or directory in the dataset.
        :project_name: whether this method should wait for the zipping process to complete beefore returning.

    Returns:
        A json representation of the path metadata.
    """
    project_id = project.project_id_as_shared(project_name)
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   project_id + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_DATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   remote_path + "?action=stat"

    response = util.send_request('GET', resource_url)
    response_object = response.json()

    if response.status_code >= 400:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get path (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))
    else:
        return json.loads(response.content)


def path_exists(remote_path, project_name=None):
    """
    Check if path exists.

    Example usage:

    >>> from hops import dataset
    >>> dataset.path_exists("Projects/project_name/Resources/myremotefile.txt")

    Args:
        :remote_path: the path to the remote file or directory in the dataset
        :project_name: whether this method should wait for the zipping process to complete beefore returning.

    Returns:
        True if path exists, False otherwise
    """
    try:
        get_path_info(remote_path, project_name)
        return True
    except RestAPIError:
        return False


def delete(remote_path, project_name=None, block=True, timeout=30):
    """
    Delete the dir or file in Hopsworks, specified by the remote_path.

    Example usage:

    >>> from hops import dataset
    >>> dataset.delete("Projects/project_name/Resources/myremotefile.txt")

    Args:
        :remote_path: the path to the remote file or directory in the dataset
        :project_name: whether this method should wait for the zipping process to complete before returning.
        :block: whether to wait for the deletion to complete or not.
        :timeout: number of seconds to wait for the deletion to complete before returning.

    Returns:
        None
    """
    project_id = project.project_id_as_shared(project_name)
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   project_id + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_DATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   remote_path

    util.send_request('DELETE', resource_url)

    # Check if path is indeed deleted as REST API is asynchronous
    if block:
        count = 0
        while count < timeout and path_exists(remote_path, project_name):
            log.debug("Waiting for deletion...")
            count += 1
            time.sleep(1)

        if count >= timeout:
            raise DeletionTimeout("Timeout of {} seconds exceeded while deleting path {}.".format(timeout, remote_path))


def compress(remote_path, project_name=None, block=False, timeout=120):
    """
    Compress the dir or file in Hopsworks, specified by the remote_path.

    Example usage:

    >>> from hops import dataset
    >>> dataset.compress("Projects/project_name/Resources/myremotefile.txt")

    Args:
        :remote_path: the path to the remote file or directory in the dataset
        :project_name: whether this method should wait for the zipping process to complete before returning.
        :block: whether to wait for the compression to complete or not.
        :timeout: number of seconds to wait for the compression to complete before returning.

    Returns:
        None
    """
    _archive(remote_path, project_name=project_name, block=block, timeout=timeout, action='zip')


def extract(remote_path, project_name=None, block=False, timeout=120):
    """
    Extract the dir or file in Hopsworks, specified by the remote_path.

    Example usage:

    >>> from hops import dataset
    >>> dataset.extract("Projects/project_name/Resources/myremotefile.zip")

    Args:
        :remote_path: the path to the remote file or directory in the dataset
        :project_name: whether this method should wait for the zipping process to complete before returning.
        :block: whether to wait for the extraction to complete or not.
        :timeout: number of seconds to wait for the extraction to complete before returning.

    Returns:
        None
    """
    _archive(remote_path, project_name=project_name, block=block, timeout=timeout, action='unzip')


def _archive(remote_path, project_name=None, block=False, timeout=120, action='zip'):
    """
    Create an archive (zip file) of a file or directory in a Hopsworks dataset.

    Args:
        :remote_path: the path to the remote file or directory in the dataset.
        :action: Allowed values are zip/unzip. Whether to compress/extract respectively.
        :block: whether this method should wait for the zipping process to complete before returning.
        :project_name: whether this method should wait for the zipping process to complete beefore returning.
        :timeout: number of seconds to wait for the action to complete before returning.
    Returns:
        None
    """
    project_id = project.project_id_as_shared(project_name)
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   project_id + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_DATASETS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   remote_path + "?action=" + action

    util.send_request('POST', resource_url)

    if block is True:
        # Wait for zip file to appear. When it does, check that parent dir zipState is not set to CHOWNING
        count = 0
        while count < timeout:
            # Get the status of the zipped file
            zip_exists = path_exists(remote_path + ".zip", project_name)
            # Get the zipState of the directory being zipped
            dir_status = get_path_info(remote_path, project_name)
            zip_state = dir_status['zipState'] if 'zipState' in dir_status else None

            if zip_exists and zip_state == 'NONE' :
                log.info("Zipping completed.")
                return
            else:
                log.info("Zipping...")
                time.sleep(1)
            count += 1
        raise CompressTimeout("Timeout of {} seconds exceeded while compressing {}.".format(timeout, remote_path))


class CompressTimeout(Exception):
    """This exception will be raised if the compression of a path times out."""


class DeletionTimeout(Exception):
    """This exception will be raised if the deletion of a path in a dataset times out."""
