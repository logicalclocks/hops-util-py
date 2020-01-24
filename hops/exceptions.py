"""
Common Exceptions thrown by the hops library
"""

class RestAPIError(Exception):
    """This exception will be raised if there is an error response from a REST API call to Hopsworks"""

class UnkownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter"""