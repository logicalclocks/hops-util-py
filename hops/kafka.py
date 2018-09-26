"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import constants

def get_schema(topic, versionId):
    """
    Gets the Avro schema for a particular Kafka topic and its version.

    Args:
        :topic: Kafka topic name
        :versionId: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    print("Getting schema for topic: {} from uri: {}".format(topic, versionId))
    return None


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

    return key_store_pwd


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