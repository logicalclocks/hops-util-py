"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import constants
from hops import tls
from hops import util
try:
    import http.client as http
except ImportError:
    import httplib as http
import ssl

def get_hopsworks_rest_endpoint():
    """
     Gets the REST endpoint of Hopsworks from OS-environment variables
    """
    return os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]

def get_host_port_pair():
    """
    Removes "http or https" from the rest endpoint and returns a list
    [endpoint, port], where endpoint is on the format /path.. without http://

    Returns:
        a list [endpoint, port]
    """
    endpoint = get_hopsworks_rest_endpoint()
    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index + 1:]
    host_port_pair = endpoint.split(':')
    return host_port_pair

def prepare_rest_appservice_json_request():
    """
    Prepares a REST JSON Request to Hopsworks APP-service

    Returns:
        a dict with keystore cert bytes and password string
    """
    key_store_pwd = tls.get_key_store_pwd()
    key_store_cert = tls.get_key_store_cert()
    json_contents = {}
    json_contents[constants.REST_CONFIG.JSON_KEYSTOREPWD] = key_store_pwd
    json_contents[constants.REST_CONFIG.JSON_KEYSTORE] = key_store_cert.decode("latin-1") # raw bytes is not serializable by JSON -_-
    return json_contents

def get_http_connection(https=False):
    """
    Opens a HTTP(S) connection to Hopsworks

    Returns:
        HTTPSConnection
    """
    host_port_pair = util.get_host_port_pair()
    if (https):
        ssl_context = ssl.SSLContext()
        connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]), context = ssl_context)
    else:
        connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    return connection