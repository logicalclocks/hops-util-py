"""
Utility functions to retrieve information about available services and setting up security for the Hops platform.

These utils facilitates development by hiding complexity for programs interacting with Hops services.
"""

import os
from hops import constants

def getKafkaDefaultConfig():
    default_config = {}
    # Configure Producer Properties
    default_config[constants.KAFKA_PRODUCER_CONFIG.BOOTSTRAP_SERVERS_CONFIG] = getBrokerEndpoints()
    default_config[constants.KAFKA_PRODUCER_CONFIG.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
    default_config[constants.KAFKA_PRODUCER_CONFIG.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.ByteArraySerializer"
    # Configure SSL Properties
    default_config[constants.KAFKA_SSL_CONFIG.SECURITY_PROTOCOL_CONFIG] = "SSL"
    default_config[constants.KAFKA_SSL_CONFIG.SSL_TRUSTSTORE_LOCATION_CONFIG] = None # TODO
    default_config[constants.KAFKA_SSL_CONFIG.SSL_TRUSTSTORE_PASSWORD_CONFIG] = None # TODO
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEYSTORE_LOCATION_CONFIG] = None # TODO
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEYSTORE_PASSWORD_CONFIG] = None # TODO
    default_config[constants.KAFKA_SSL_CONFIG.SSL_KEY_PASSWORD_CONFIG] = None # TODO

    return default_config

def getConsumerConfig():
    None

def getSchema(topic):
    """
    Gets the Avro schema for a particular Kafka topic with version 1

    Args:
    :topic: Kafka topic name
    :versionId: Schema version ID

    Returns:
        Avro schema as a string object in JSON format
    """
    return getSchema(topic, 1)

def getSchema(topic, versionId):
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

def getBrokerEndpointsList():
    """
    Get Kafka broker endpoints as a list

    Returns:
        a list with broker endpoint strings
    """
    return os.environ[constants.ENV_VARIABLES.KAFKA_BROKERS_ENV].split(",")

def getBrokerEndpoints():
    """
    Get Kafka broker endpoints as a string with broker-endpoints "," separated

    Returns:
        a string with broker endpoints comma-separated
    """
    return os.environ[constants.ENV_VARIABLES.KAFKA_BROKERS_ENV]

def getTopics():
    return None

def getTopicAsCSV():
    return None

def getRestEndpoint():
    return None

def getKeyStore():
    return None

def getTrustStore():
    return None

def getKeystorePwd():
    return None

def getTruststorePwd():
    return None

def getProjectId():
    return None

def getJobName():
    return None

def getJobType():
    return None
