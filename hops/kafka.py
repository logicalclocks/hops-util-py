"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import constants
from hops import util
from hops import hdfs
import string
import base64

try:
    import http.client as http
except ImportError:
    import httplib as http

def prepare_rest_json_request():
    key_store_pwd = get_key_store_pwd()
    key_store_cert = get_key_store_cert()
    json_contents = {}
    json_contents[constants.REST_CONFIG.JSON_KEYSTOREPWD] = key_store_pwd
    json_contents[constants.REST_CONFIG.JSON_KEYSTORE] = key_store_cert
    return json_contents

def get_host_port_pair():
    """
    Removes "http or https" from the rest endpoint and returns a list
    [endpoint, port], where endpoint is on the format /path.. without http://

    Returns:
        a list [endpoint, port]
    """
    endpoint = util.get_hopsworks_rest_endpoint()
    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index+1:]
    host_port_pair = endpoint.split(':')
    return host_port_pair

def get_http_connection(https=False):
    """
    Opens a HTTP(S) connection to Hopsworks

    Returns:
        HTTPSConnection
    """
    host_port_pair = get_host_port_pair()
    if(https):
        connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    else:
        http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    return connection


def get_schema(topic, version_id):
    """
    Gets the Avro schema for a particular Kafka topic and its version.

    Args:
        :topic: Kafka topic name
        :version_id: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    print("Getting schema for topic: {} schema version: {}".format(topic, version_id))
    json = prepare_rest_json_request()
    json[constants.REST_CONFIG.JSON_SCHEMA_TOPICNAME] = topic
    json[constants.REST_CONFIG.JSON_SCHEMA_VERSION] = version_id
    json_embeddable = json.dumps(json)
    headers = {'Content-type': 'application/json'}
    method = "GET"
    connection = get_http_connection()
    resource = "schema"
    resource_url = constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + "/" + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + "/" + resource
    print("Sending REST request to Hopsworks: {}".format(resource_url))
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    return response_object


def get_schema(topic):
    """
    Gets the Avro schema for a particular Kafka topic with version 1

    Args:
    :topic: Kafka topic name
    :versionId: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    return get_schema(topic, 1)


def get_broker_endpoints():
    """
    Get Kafka broker endpoints as a string with broker-endpoints "," separated

    Returns:
        a string with broker endpoints comma-separated
    """
    return os.environ[constants.ENV_VARIABLES.KAFKA_BROKERS_ENV_VAR]

def get_broker_endpoints_list():
    """
    Get Kafka broker endpoints as a list

    Returns:
        a list with broker endpoint strings
    """
    return get_broker_endpoints().split(",")


def get_key_store():
    """
    Get keystore location

    Returns:
        keystore filename
    """
    return constants.SSL_CONFIG.K_CERTIFICATE_CONFIG


def get_trust_store():
    """
    Get truststore location

    Returns:
         truststore filename
    """
    return constants.SSL_CONFIG.T_CERTIFICATE_CONFIG


def _get_cert_pw():
    """
    Get keystore password from local container

    Returns:
        Certificate password
    """
    pwd_path = os.getcwd() + "/" + constants.SSL_CONFIG.CRYPTO_MATERIAL_PASSWORD

    if not os.path.exists(pwd_path):
        raise AssertionError('material_passwd is not present in directory: {}'.format(pwd_path))

    with open(pwd_path) as f:
        key_store_pwd = f.read()

    # remove special characters (due to bug in password materialized, should not be necessary when the bug is fixed)
    key_store_pwd = "".join(list(filter(lambda x: x in string.printable and not x == "@", key_store_pwd)))
    return key_store_pwd

def get_key_store_cert():
    """
    Get keystore certificate from local container

    Returns:
        Certificate password
    """
    cert_path = os.getcwd() + "/" + constants.SSL_CONFIG.K_CERTIFICATE_CONFIG

    if not os.path.exists(cert_path):
        raise AssertionError('k_certificate is not present in directory: {}'.format(cert_path))

    with open(cert_path) as f:
        key_store_cert = f.read()
        key_store_cert = base64.b64encode(key_store_cert)

    return key_store_cert


def get_key_store_pwd():
    """
    Get keystore password

    Returns:
         keystore password
    """
    return _get_cert_pw()


def get_trust_store_pwd():
    """
    Get truststore password

    Returns:
         truststore password
    """
    return _get_cert_pw()

def get_kafka_default_config():
    """
    Gets a default configuration for running secure Kafka on Hops

    Returns:
         dict with config_property --> value
    """
    default_config = {}
    # Configure Producer Properties
    default_config[constants.KAFKA_PRODUCER_CONFIG.BOOTSTRAP_SERVERS_CONFIG] = get_broker_endpoints()
    default_config[constants.KAFKA_PRODUCER_CONFIG.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
    default_config[constants.KAFKA_PRODUCER_CONFIG.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.ByteArraySerializer"
    # Configure SSL Properties
    default_config[constants.KAFKA_SSL_CONFIG.SECURITY_PROTOCOL_CONFIG] = "SSL"
    default_config[constants.KAFKA_SSL_CONFIG.SSL_TRUSTSTORE_LOCATION_CONFIG] = get_trust_store()
    default_config[constants.KAFKA_SSL_CONFIG.SSL_TRUSTSTORE_PASSWORD_CONFIG] = get_trust_store_pwd()
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEYSTORE_LOCATION_CONFIG] = get_key_store()
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEYSTORE_PASSWORD_CONFIG] = get_key_store_pwd()
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEY_PASSWORD_CONFIG] = get_key_store_pwd()

    return default_config