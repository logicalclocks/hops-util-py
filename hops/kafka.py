"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import constants
from hops import tls
from hops import rest_api
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

def get_schema(topic, version_id=1):
    """
    Gets the Avro schema for a particular Kafka topic and its version.

    Args:
        :topic: Kafka topic name
        :version_id: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    json_contents = rest_api.prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_SCHEMA_TOPICNAME] = topic
    json_contents[constants.REST_CONFIG.JSON_SCHEMA_VERSION] = version_id
    json_embeddable = json.dumps(json_contents)
    headers = {'Content-type': 'application/json'}
    method = "POST"
    connection = rest_api.get_http_connection(https=True)
    resource = "schema"
    resource_url = "/" + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + "/" + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + "/" + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    return response_object
