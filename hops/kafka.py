"""
A module for setting up Kafka Brokers and Consumers on the Hops platform. It hides the complexity of
configuration Kafka by providing utility methods such as `get_broker_endpoints()`, `get_security_protocol`,
 `get_kafka_default_config` etc.
"""

import os
from hops import constants
from hops import tls
from hops import util
import json

def get_broker_endpoints():
    """
    Get Kafka broker endpoints as a string with broker-endpoints "," separated

    Returns:
        a string with broker endpoints comma-separated
    """
    return os.environ[constants.ENV_VARIABLES.KAFKA_BROKERS_ENV_VAR].replace("INTERNAL://","")

def get_security_protocol():
    """
    Gets the security protocol used for communicating with Kafka brokers in a Hopsworks cluster

    Returns:
        the security protocol for communicating with Kafka brokers in a Hopsworks cluster
    """
    return "SSL"


def get_broker_endpoints_list():
    """
    Get Kafka broker endpoints as a list

    Returns:
        a list with broker endpoint strings
    """
    return get_broker_endpoints().split(",")

def get_kafka_default_config():
    """
    Gets a default configuration for running secure Kafka on Hops

    Returns:
         dict with config_property --> value
    """
    default_config = {
        constants.KAFKA_PRODUCER_CONFIG.BOOTSTRAP_SERVERS_CONFIG: get_broker_endpoints(),
        constants.KAFKA_SSL_CONFIG.SECURITY_PROTOCOL_CONFIG: get_security_protocol(),
        constants.KAFKA_SSL_CONFIG.SSL_CA_LOCATION_CONFIG: tls.get_ca_chain_location(),
        constants.KAFKA_SSL_CONFIG.SSL_CERTIFICATE_LOCATION_CONFIG: tls.get_client_certificate_location(),
        constants.KAFKA_SSL_CONFIG.SSL_PRIVATE_KEY_LOCATION_CONFIG: tls.get_client_key_location(),
        "group.id": "something"
    }
    return default_config

def _prepare_rest_appservice_json_request():
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

def get_schema(topic, version_id=1):
    """
    Gets the Avro schema for a particular Kafka topic and its version.

    Args:
        :topic: Kafka topic name
        :version_id: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    json_contents = _prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_SCHEMA_TOPICNAME] = topic
    json_contents[constants.REST_CONFIG.JSON_SCHEMA_VERSION] = version_id
    json_embeddable = json.dumps(json_contents)
    headers = {'Content-type': 'application/json'}
    method = "POST"
    connection = util.get_http_connection(https=True)
    resource = "schema"
    resource_url = "/" + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + "/" + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + "/" + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    return response_object
