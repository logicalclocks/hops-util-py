"""
A module for working with Hopsworks datasets.
"""

import re
import os
import ntpath
import math
from hops import constants, util
from hops.exceptions import RestAPIError


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

        progress = round(self.params['flowChunkNumber'] / self.params['flowTotalChunks'], 3) * 100
        print("Progress: " + str(progress) + "%")
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

    Returns:
        None
    """
    HTTPUpload(file, remote_path, chunk_size).upload()
