"""
A module for setting up Kafka Brokers and Consumers on the Hops platform. It hides the complexity of
configuring Kafka by providing utility methods such as:

    - `get_broker_endpoints()`.
    - `get_security_protocol()`.
    - `get_kafka_default_config()`.
    - etc.

Using these utility functions you can setup Kafka with the Kafka client-library of your choice, e.g SparkStreaming or
confluent-kafka-python. For example, assuming that you have created a topic called "test" on Hopsworks and that you
have installed confluent-kafka-python inside your project's anaconda environment:

    >>> from hops import kafka
    >>> from confluent_kafka import Producer, Consumer
    >>> TOPIC_NAME = "test"
    >>> config = kafka.get_kafka_default_config()
    >>> producer = Producer(config)
    >>> consumer = Consumer(config)
    >>> consumer.subscribe(["test"])
    >>> # wait a little while before executing the rest of the code (put it in a different Jupyter cell)
    >>> # so that the consumer get chance to subscribe (asynchronous call)
    >>> for i in range(0, 10):
    >>> producer.produce(TOPIC_NAME, "message {}".format(i), "key", callback=delivery_callback)
    >>> # Trigger the sending of all messages to the brokers, 10sec timeout
    >>> producer.flush(10)
    >>> for i in range(0, 10):
    >>> msg = consumer.poll(timeout=5.0)
    >>> if msg is not None:
    >>>     print('Consumed Message: {} from topic: {}'.format(msg.value(), msg.topic()))
    >>> else:
    >>>     print("Topic empty, timeout when trying to consume message")


Similarly, you can define a pyspark kafka consumer as follows, using the spark session defined in variable `spark`

    >>> from hops import kafka
    >>> from hops import tls
    >>> TOPIC_NAME = "test"
    >>> df = spark \.format("kafka")
    >>> .option("kafka.bootstrap.servers", kafka.get_broker_endpoints())
    >>> .option("kafka.ssl.truststore.location", tls.get_trust_store())
    >>> .option("kafka.ssl.truststore.password", tls.get_key_store_pwd())
    >>> .option("kafka.ssl.keystore.location", tls.get_key_store())
    >>> .option("kafka.ssl.keystore.password", tls.get_key_store_pwd())
    >>> .option("kafka.ssl.key.password", tls.get_trust_store_pwd())
    >>> .option("subscribe", TOPIC_NAME)
    >>> .load()
"""

import os
from hops import constants, tls, util, hdfs
from hops.exceptions import RestAPIError
import json
import sys

# for backwards compatibility
try:
    from ast import literal_eval
    from io import BytesIO
    from avro.io import DatumReader, BinaryDecoder
    import avro.schema
except:
    pass

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
    return constants.KAFKA_SSL_CONFIG.SSL


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


def get_schema(topic):
    """
    Gets the Avro schema for a particular Kafka topic.

    Args:
        :topic: Kafka topic name

    Returns:
        Avro schema as a string object in JSON format
    """
    method = constants.HTTP_CONFIG.HTTP_GET
    resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_PROJECT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   hdfs.project_id() + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_KAFKA_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_TOPICS_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                   topic + constants.DELIMITERS.SLASH_DELIMITER + \
                   constants.REST_CONFIG.HOPSWORKS_SUBJECTS_RESOURCE
    response = util.send_request(method, resource_url)
    response_object = response.json()

    if response.status_code != 200:
        error_code, error_msg, user_msg = util._parse_rest_error(response_object)
        raise RestAPIError("Could not get Avro schema (url: {}), server response: \n "
                           "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
            resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

    return response_object['schema']


def parse_avro_msg(msg, avro_schema):
    """
    Parses an avro record using a specified avro schema

    Args:
        :msg: the avro message to parse
        :avro_schema: the avro schema

    Returns:
         The parsed/decoded message
    """
    reader = DatumReader(avro_schema)
    message_bytes = BytesIO(msg)
    decoder = BinaryDecoder(message_bytes)
    return reader.read(decoder)


def convert_json_schema_to_avro(json_schema):
    """
    Parses a JSON kafka topic schema returned by Hopsworks REST API into an avro schema

    Args:
       :json_schema: the json schema to convert

    Returns:
         the avro schema
    """

    return avro.schema.parse(json_schema)



class KafkaTopicDTO(object):
    """
    Represents a KafkaTopic in Hopsworks
    """

    def __init__(self, kafka_topic_dto_json):
        """
        Initialize the kafka topic from JSON payload returned by Hopsworks REST API

        Args:
            :kafka_topic_dto_json: JSON data about the kafka topic returned from Hopsworks REST API
        """
        self.name = kafka_topic_dto_json[constants.REST_CONFIG.JSON_KAFKA_TOPIC_NAME]
        self.schema_version = kafka_topic_dto_json[constants.REST_CONFIG.JSON_KAFKA_TOPIC_SCHEMA_VERSION]
